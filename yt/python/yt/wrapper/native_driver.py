from .config import get_config, get_option, set_option, get_backend_type
from .common import require, generate_int64, update, get_value
from .errors import create_response_error, YtError
from .string_iter_io import StringIterIO
from .response_stream import ResponseStream
from .http_helpers import get_proxy_url, get_token

import yt.logger as logger
import yt.logger_config as logger_config
import yt.yson as yson

from yt.packages.six import binary_type

try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO


backend_to_driver_bindings = {}


class DriverBindings(object):
    def __init__(self, bindings, backend_type):
        self.bindings = bindings
        self.backend_type = backend_type
        self.logging_configured = False
        self.address_resolver_configured = False


class NullStream(object):
    def write(self, data):
        pass

    def close(self):
        pass


def lazy_import_driver_bindings(backend_type, allow_fallback_to_native_driver):
    global backend_to_driver_bindings
    if backend_type in backend_to_driver_bindings:
        return

    if backend_type == "rpc":
        try:
            import yt_driver_rpc_bindings
            backend_to_driver_bindings["rpc"] = DriverBindings(yt_driver_rpc_bindings, "rpc")
        except ImportError:
            if allow_fallback_to_native_driver:
                try:
                    import yt_driver_bindings
                    backend_to_driver_bindings["native"] = DriverBindings(yt_driver_bindings, "native")
                except ImportError:
                    pass
    else:
        try:
            import yt_driver_bindings
            backend_to_driver_bindings[backend_type] = DriverBindings(yt_driver_bindings, backend_type)
        except ImportError:
            pass


def read_config(path):
    driver_config = yson.load(open(path, "rb"))
    if "driver" in driver_config:
        return (
            driver_config["driver"],
            driver_config.get("logging"),
            driver_config.get("address_resolver"),
        )
    else:
        return (
            driver_config,
            None,
            None
        )


def configure_logging(logging_config_from_file, client):
    backend_type = get_backend_type(client)

    global backend_to_driver_bindings
    if backend_to_driver_bindings[backend_type].logging_configured:
        return

    config = get_config(client)
    if config["driver_logging_config"]:
        backend_to_driver_bindings[backend_type].bindings.configure_logging(config["driver_logging_config"])
    elif logging_config_from_file:
        backend_to_driver_bindings[backend_type].bindings.configure_logging(logging_config_from_file)
    else:
        if logger_config.LOG_LEVEL is None:
            min_level = "warning"
        else:
            min_level = logger_config.LOG_LEVEL.lower()

        logging_config = {
            "rules": [
                {
                    "min_level": min_level,
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
        backend_to_driver_bindings[backend_type].bindings.configure_logging(logging_config)

    backend_to_driver_bindings[backend_type].logging_configured = True


def configure_address_resolver(address_resolver_config, client):
    backend_type = get_backend_type(client)
    global backend_to_driver_bindings
    if backend_to_driver_bindings[backend_type].address_resolver_configured:
        return

    config = get_config(client)
    if config["driver_address_resolver_config"] is not None:
        backend_to_driver_bindings[backend_type].bindings.configure_address_resolver(config["driver_address_resolver_config"])
    elif address_resolver_config is not None:
        backend_to_driver_bindings[backend_type].bindings.configure_address_resolver(address_resolver_config)

    backend_to_driver_bindings[backend_type].address_resolver_configured = True


def get_driver_instance(client):
    driver = get_option("_driver", client=client)
    backend_type = get_backend_type(client)
    if driver is None:
        logging_config = None
        address_resolver_config = None

        config = get_config(client)
        if config["driver_config"] is not None:
            driver_config = config["driver_config"]
        elif config["driver_config_path"] is not None:
            driver_config, logging_config, address_resolver_config = \
                read_config(config["driver_config_path"])
        else:
            if backend_type == "rpc":
                if config["proxy"]["url"] is None:
                    raise YtError("For rpc backend driver config or proxy url must be specified")
                else:
                    driver_config = {}
            else:
                raise YtError("Driver config is not specified")

        if config["backend"] == "rpc":
            if driver_config.get("connection_type") is None:
                driver_config = update(
                    {"connection_type": "rpc", "cluster_url": "http://" + get_proxy_url(client=client)},
                    driver_config)
            elif config["backend"] != driver_config["connection_type"]:
                raise YtError(
                    "Driver connection type and client backend mismatch "
                    "(driver_connection_type: {0}, client_backend: {1})"
                    .format(driver_config["connection_type"], config["backend"]))

        lazy_import_driver_bindings(backend_type, config["allow_fallback_to_native_driver"])
        if backend_type not in backend_to_driver_bindings:
            if backend_type == "rpc":
                raise YtError("Driver class not found, install RPC driver bindings. "
                              "Bindings are shipped as additional package and "
                              "can be installed as Debian package \"yandex-yt-python-driver-rpc\" "
                              "or as pip package \"yandex-yt-driver-rpc-bindings\"")
            else:
                raise YtError("Driver class not found, install native yt driver bindings")

        configure_logging(logging_config, client)
        configure_address_resolver(address_resolver_config, client)

        specified_api_version = get_value(
            get_config(client)["api_version"],
            get_config(client)["default_api_version_for_rpc"])
        if "api_version" in driver_config:
            if specified_api_version is not None and "v" + str(driver_config["api_version"]) != specified_api_version:
                raise YtError(
                    "Version specified in driver config and client config do not match "
                    "(client_config: {}, driver_config: {})"
                    .format(specified_api_version, driver_config["api_version"]))
        else:
            if specified_api_version is not None:
                driver_config["api_version"] = int(specified_api_version[1:])

        set_option("_driver", backend_to_driver_bindings[backend_type].bindings.Driver(driver_config), client=client)
        driver = get_option("_driver", client=client)

    return driver


def create_driver_for_cell(driver, cell_id, backend_type):
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
    if "master_cache" in config:
        del config["master_cache"]
    if "timestamp_provider" in config:
        del config["timestamp_provider"]

    del config["secondary_masters"]

    return backend_to_driver_bindings[backend_type].bindings.Driver(config)


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
                raise create_response_error(response.error())
            else:
                break
        yield stream.read(size)

    while not stream.empty():
        yield stream.read(size)


def make_request(command_name, params,
                 data=None,
                 return_content=True,
                 client=None):
    driver = get_driver_instance(client)
    backend_type = get_backend_type(client)

    cell_id = params.get("master_cell_id")
    if cell_id is not None:
        driver = create_driver_for_cell(driver, cell_id, backend_type)

    require(command_name in driver.get_command_descriptors(),
            lambda: YtError("Command {0} is not supported".format(command_name)))

    description = driver.get_command_descriptor(command_name)

    input_stream = convert_to_stream(data)

    output_stream = None
    if description.output_type() != b"Null":
        if "output_format" not in params and description.output_type() != b"Binary":
            output_stream = NullStream()
            params["output_format"] = "yson"
            # TODO(ignat): return this error after full migration to v4.
            # raise YtError(
            #     "Inner error: output format is not specified for native driver command '{0}'"
            #     .format(command_name))
        else:
            if return_content:
                output_stream = BytesIO()
            else:
                output_stream = backend_to_driver_bindings[backend_type].bindings.BufferedStream(size=get_config(client)["read_buffer_size"])

    request_id = generate_int64(get_option("_random_generator", client))

    logger.debug("Executing command %s with parameters %s and id %s", command_name, repr(params), hex(request_id)[2:])

    driver_user_name = get_config(client)["driver_user_name"]
    if driver_user_name is not None:
        driver_user_name = str(driver_user_name)

    token = None
    service_ticket = None

    tvm_auth = get_config(client)["tvm_auth"]
    if tvm_auth is not None:
        service_ticket = tvm_auth.issue_service_ticket()
    else:
        token = get_token(client=client)

    try:
        request = backend_to_driver_bindings[backend_type].bindings.Request(
            command_name=command_name,
            parameters=params,
            input_stream=input_stream,
            output_stream=output_stream,
            user=driver_user_name,
            token=token,
            service_ticket=service_ticket)
    except TypeError:
        request = backend_to_driver_bindings[backend_type].bindings.Request(
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
            error = create_response_error(response.error())
            error.message = "Received driver response with error"
            raise error
        if output_stream is not None and not isinstance(output_stream, NullStream):
            value = output_stream.getvalue()
            return value
    else:
        def process_error(request):
            if response.is_set() and not response.is_ok():
                raise create_response_error(response.error())

        if response.is_set() and not response.is_ok():
            raise create_response_error(response.error())

        return ResponseStream(
            lambda: response,
            chunk_iter(output_stream, response, get_config(client)["read_buffer_size"]),
            lambda from_delete: None,
            process_error,
            lambda: response.response_parameters())
