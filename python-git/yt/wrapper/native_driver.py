from .config import get_config, get_option, set_option
from .common import require, generate_int64
from .errors import YtResponseError, YtError
from .string_iter_io import StringIterIO
from .response_stream import ResponseStream
from .http_helpers import get_proxy_url, get_token

import yt.logger as logger
import yt.yson as yson

from yt.packages.six import binary_type, PY3

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

driver_bindings = None
def lazy_import_driver_bindings(backend_type, allow_fallback_to_native_driver):
    global driver_bindings
    if driver_bindings is not None:
        return

    if backend_type == "rpc":
        try:
            import yt_driver_rpc_bindings
            driver_bindings = yt_driver_rpc_bindings
        except ImportError:
            if allow_fallback_to_native_driver:
                try:
                    import yt_driver_bindings
                    driver_bindings = yt_driver_bindings
                except ImportError:
                    pass
    else:
        try:
            import yt_driver_bindings
            driver_bindings = yt_driver_bindings
        except ImportError:
            pass

def read_config(path):
    driver_config = yson.load(open(path, "rb"))
    return (
        driver_config["driver"],
        driver_config.get("logging"),
        driver_config.get("tracing"),
        driver_config.get("address_resolver"),
    )

logging_configured = False
def configure_logging(logging_config_from_file, client):
    global logging_configured
    if logging_configured:
        return

    config = get_config(client)
    if config["driver_logging_config"] is not None:
        driver_bindings.configure_logging(config["driver_logging_config"])
    elif config["enable_driver_logging_to_stderr"]:
        logging_config = {
            "rules": [
                {
                    "min_level": "debug",
                    "writers": [
                        "stderr",
                    ],
                },
            ],
            "writers": {
                "stderr": {
                    "type": "stderr",
                },
            },
        }
        driver_bindings.configure_logging(logging_config)
    elif logging_config_from_file is not None:
        driver_bindings.configure_logging(logging_config_from_file)
    else:
        driver_bindings.configure_logging({"rules": [], "writers": {}})

    logging_configured = True

tracing_configured = False
def configure_tracing(tracing_config_from_file, client):
    global tracing_configured
    if tracing_configured:
        return

    if tracing_config_from_file is not None:
        driver_bindings.configure_tracing(tracing_config_from_file)

    tracing_configured = True

address_resolver_configured = False
def configure_address_resolver(address_resolver_config, client):
    global address_resolver_configured
    if address_resolver_configured:
        return

    config = get_config(client)
    if config["driver_address_resolver_config"] is not None:
        driver_bindings.configure_address_resolver(config["driver_address_resolver_config"])
    elif address_resolver_config is not None:
        driver_bindings.configure_address_resolver(address_resolver_config)

    address_resolver_configured = True

def get_driver_instance(client):
    driver = get_option("_driver", client=client)
    if driver is None:
        logging_config = None
        tracing_config = None
        address_resolver_config = None

        config = get_config(client)
        if config["driver_config"] is not None:
            driver_config = config["driver_config"]
        elif config["driver_config_path"] is not None:
            driver_config, logging_config, tracing_config, address_resolver_config = \
                read_config(config["driver_config_path"])
        else:
            if config["backend"] == "rpc":
                if config["proxy"]["url"] is None:
                    raise YtError("For rpc backend driver config or proxy url must be specified")
                else:
                    driver_config = {"connection_type": "rpc", "cluster_url": "http://" + get_proxy_url(client=client)}
            else:
                raise YtError("Driver config is not specified")

        if config["backend"] == "rpc":
            if driver_config.get("connection_type") is None:
                driver_config["connection_type"] = "rpc"
            elif config["backend"] != driver_config["connection_type"]:
                raise YtError(
                    "Driver connection type and client backend mismatch "
                    "(driver_connection_type: {0}, client_backend: {1})"
                    .format(driver_config["connection_type"], config["backend"]))

        lazy_import_driver_bindings(config["backend"], config["allow_fallback_to_native_driver"])
        if driver_bindings is None:
            raise YtError("Driver class not found, install yt driver bindings.")

        configure_logging(logging_config, client)
        configure_tracing(tracing_config, client)
        configure_address_resolver(address_resolver_config, client)

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
            raise YtError(
                "Inner error: output format is not specified for native driver command '{0}'"
                .format(command_name))
        if return_content:
            output_stream = BytesIO()
        else:
            output_stream = driver_bindings.BufferedStream(size=get_config(client)["read_buffer_size"])

    request_id = generate_int64(get_option("_random_generator", client))

    logger.debug("Executing command %s with parameters %s and id %s", command_name, repr(params), hex(request_id)[2:])

    driver_user_name = get_config(client)["driver_user_name"]
    if driver_user_name is not None:
        driver_user_name = str(driver_user_name)

    try:
        request = driver_bindings.Request(
            command_name=command_name,
            parameters=params,
            input_stream=input_stream,
            output_stream=output_stream,
            user=driver_user_name,
            token=get_token(client=client))
    except TypeError:
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
