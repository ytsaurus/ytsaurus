from .config import get_config, get_option, set_option, get_backend_type
from .common import require, generate_uuid, update, get_value
from .constants import RPC_PACKAGE_INSTALLATION_TEXT, ENABLE_YP_SERVICE_DISCOVERY
from .default_config import DefaultConfigRetriesType
from .errors import create_response_error, YtError
from .retries import Retrier
from .string_iter_io import StringIterIO
from .telemetry import _telemetry
from .response_stream import ResponseStream
from .http_helpers import get_proxy_address_url, get_cluster_name, get_token, get_retriable_errors, format_logging_params, _MakeRequestParams

from yt.common import _pretty_format_for_logging

import yt.logger as logger
import yt.logger_config as logger_config
import yt.yson as yson

import inspect
import os
import re
import typing

from copy import deepcopy
from io import BytesIO


if typing.TYPE_CHECKING:
    import yt_driver_rpc_bindings  # noqa


driver_bindings = None
driver_bindings_type = None
driver_bindings_imported_with_pid = None
logging_configured = False
address_resolver_configured = False
yp_service_discovery_configured = False


class NullStream(object):
    def write(self, data):
        pass

    def close(self):
        pass


def lazy_import_driver_bindings():
    global driver_bindings
    global driver_bindings_type
    global driver_bindings_imported_with_pid

    if driver_bindings is not None:
        return

    try:
        import yt_driver_bindings
        driver_bindings = yt_driver_bindings
        driver_bindings_type = "native"
        driver_bindings_imported_with_pid = os.getpid()
        return
    except ImportError:
        pass

    try:
        import yt_driver_rpc_bindings  # noqa
        driver_bindings = yt_driver_rpc_bindings
        driver_bindings_type = "rpc"
        driver_bindings_imported_with_pid = os.getpid()
        return
    except ImportError:
        pass


def create_driver(config, connection_type):
    global driver_bindings
    global driver_bindings_type

    try:
        return driver_bindings.Driver(config, connection_type=connection_type)
    except RuntimeError as ex:
        if "Excessive named argument \'connection_type\'" not in str(ex):
            raise
        if connection_type == "native" and driver_bindings_type == "rpc":
            raise RuntimeError("Cannot create native driver with RPC driver bindings")
        return driver_bindings.Driver(config)


def read_config(path):
    with open(path, "rb") as inf:
        driver_config = yson.load(inf)
    if "driver" in driver_config:
        return (
            driver_config["driver"],
            driver_config.get("logging"),
            driver_config.get("address_resolver"),
            driver_config.get("yp_service_discovery"),
        )
    else:
        return (
            driver_config,
            None,
            None,
            None
        )


def configure_logging(logging_config_from_file, client):
    global logging_configured
    if logging_configured:
        return

    config = get_config(client)
    if config["driver_logging_config"]:
        driver_bindings.configure_logging(config["driver_logging_config"])
    elif logging_config_from_file:
        driver_bindings.configure_logging(logging_config_from_file)
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
                        "log_stream",
                    ],
                },
            ],
            "writers": {
                "log_stream": None,
            },
        }

        if logger_config.LOG_PATH:
            logging_config["writers"]["log_stream"] = {
                "type": "file",
                "file_name": logger_config.LOG_PATH,
            }
        else:
            logging_config["writers"]["log_stream"] = {
                "type": "stderr",
            }

        driver_bindings.configure_logging(logging_config)

    logging_configured = True


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


def configure_yp_service_discovery(yp_service_discovery_config, client):
    global yp_service_discovery_configured
    if yp_service_discovery_configured:
        return

    config = get_config(client)
    if config["yp_service_discovery_config"] is not None:
        driver_bindings.configure_yp_service_discovery(config["yp_service_discovery_config"])
    elif yp_service_discovery_config is not None:
        driver_bindings.configure_yp_service_discovery(yp_service_discovery_config)
    else:
        driver_bindings.configure_yp_service_discovery({"enable": ENABLE_YP_SERVICE_DISCOVERY})

    yp_service_discovery_configured = True


def get_driver_instance(client):
    driver = get_option("_driver", client=client)
    if driver is None:
        logging_config = None
        address_resolver_config = None
        yp_service_discovery_config = None

        client_config = get_config(client)
        client_backend = get_backend_type(client)
        if client_config["driver_config"] is not None:
            driver_config = client_config["driver_config"]
        elif client_config["driver_config_path"] is not None:
            driver_config, logging_config, address_resolver_config, yp_service_discovery_config = \
                read_config(client_config["driver_config_path"])
        else:
            if client_backend == "rpc":
                if client_config["proxy"]["url"] is None:
                    raise YtError("For rpc backend driver config or proxy url must be specified")
                else:
                    driver_config = {}
            else:
                raise YtError("Driver config is not specified")

        if client_backend == "rpc":
            if driver_config.get("connection_type") is None:
                driver_config_patch = None

                if client_config["enable_rpc_proxy_in_job_proxy"] and os.environ.get("YT_JOB_PROXY_SOCKET_PATH"):
                    cluster_name = client_config["cluster_name_for_rpc_proxy_in_job_proxy"] or get_cluster_name(client=client)
                    if re.search(r"\W", cluster_name):
                        logger.debug(f"Do not enabling local rpc mode (wrong cluster name: \"{cluster_name}\")")
                    else:

                        socket_path = os.environ.get("YT_JOB_PROXY_SOCKET_PATH")
                        if not socket_path or not os.path.exists(socket_path):
                            raise RuntimeError(f"Socket for local rpc proxy does not exists \"{socket_path}\"")

                        driver_config_patch = {
                            "connection_type": "rpc",
                            "multiproxy_target_cluster": cluster_name,
                            "proxy_unix_domain_socket": socket_path,
                        }

                        logger.debug("Switching to local rpc proxy mode")

                if not driver_config_patch:
                    driver_config_patch = {
                        "connection_type": "rpc",
                        "cluster_url": get_proxy_address_url(client=client)
                    }

                driver_config = update(
                    driver_config_patch,
                    driver_config,
                )
            elif client_backend != driver_config["connection_type"]:
                raise YtError(
                    "Driver connection type and client backend mismatch "
                    "(driver_connection_type: {0}, client_backend: {1})"
                    .format(driver_config["connection_type"], client_backend))

        if client_config["proxy"]["rpc_proxy_role"] is not None:
            driver_config.setdefault("proxy_role", client_config["proxy"]["rpc_proxy_role"])

        lazy_import_driver_bindings()
        if driver_bindings is None:
            if client_backend == "rpc":
                raise YtError("Driver class not found, install RPC driver bindings. "
                              "Bindings are shipped as additional package and "
                              "can be installed " + RPC_PACKAGE_INSTALLATION_TEXT)
            else:
                raise YtError("Driver class not found, install native yt driver bindings")

        configure_logging(logging_config, client)
        configure_address_resolver(address_resolver_config, client)
        configure_yp_service_discovery(yp_service_discovery_config, client)

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

        if client_backend == "http":
            connection_type = driver_bindings_type
        else:
            connection_type = client_backend

        set_option("_driver", create_driver(driver_config, connection_type), client=client)
        driver = get_option("_driver", client=client)

    if driver_bindings_imported_with_pid is not None and driver_bindings_imported_with_pid != os.getpid():
        logger.error("RPC driver bindings used from different processes and may no function properly; it usually means that your program uses multiprocessong module (original_pid: %s, current_pid: %s)", driver_bindings_imported_with_pid, os.getpid())  # noqa

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
    if "master_cache" in config:
        del config["master_cache"]
    if "timestamp_provider" in config:
        del config["timestamp_provider"]
    if "cypress_proxy" in config:
        del config["cypress_proxy"]

    del config["secondary_masters"]

    return create_driver(config, connection_type="native")


def convert_to_stream(data):
    if data is None:
        return data
    elif hasattr(data, "read"):
        return data
    elif isinstance(data, bytes):
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


class RpcRequestRetrier(Retrier):
    def __init__(
        self,
        driver,  # type: yt_driver_rpc_bindings.Driver
        request,  # type: yt_driver_rpc_bindings.Request
        return_content: bool,
        make_retries: typing.Optional[bool],
        retry_action,  # type: typing.Callable[[YtError, yt_driver_rpc_bindings.Request], None]
        retry_config: typing.Optional[DefaultConfigRetriesType],
        client,
    ):
        self.driver = driver
        self.request = request
        self.return_content = return_content
        self.make_retries = True if make_retries is None else make_retries
        self.retry_action = retry_action
        self.telemetry = client._telemetry if client else _telemetry

        retriable_errors = get_retriable_errors()
        non_retriable_errors = tuple()
        if not retry_config:
            retry_config = deepcopy(get_config(client)["proxy"]["retries"])

        self.telemetry.transport.call_count += 1

        super(RpcRequestRetrier, self).__init__(
            exceptions=retriable_errors,
            ignore_exceptions=non_retriable_errors,
            retry_config=retry_config,
        )

    def action(self):
        self.telemetry.transport.requests_count += 1

        response = self.driver.execute(self.request)

        if self.return_content:
            response.wait()

            if not response.is_ok():
                error = create_response_error(response.error())
                error.message = "Received driver response with error"
                raise error
        else:
            if response.is_set() and not response.is_ok():
                raise create_response_error(response.error())

        return response

    def except_action(self, error, attempt):
        logging_params = {
            "rpc_request_id": self.request.id,
        }
        if isinstance(error, YtError):
            try:
                logging_params["full_error"] = _pretty_format_for_logging(error)
            except Exception:
                logger.exception("Failed to format error")

        logger.warning(
            "RPC method \"%s\" failed with error \"%s\" (%s)",
            self.request.command_name,
            repr(error),
            format_logging_params(logging_params),
        )

        if self.make_retries:
            self.telemetry.transport.retries_count += 1

            if self.retry_action is not None:
                self.retry_action(error, self.request)
        else:
            self.telemetry.transport.fail_count += 1

            raise error


def make_request(
    command_name,
    params,
    data=None,
    return_content=True,
    allow_retries=None,
    retry_config=None,
    mutation_id=None,
    client=None
):
    driver = get_driver_instance(client)

    cell_id = params.get("master_cell_id")
    if cell_id is not None:
        driver = create_driver_for_cell(driver, cell_id)

    commands = driver.get_command_descriptors()

    request_params = _MakeRequestParams(command_name, allow_retries, mutation_id, commands, client)

    command = request_params.command
    require(command,
            lambda: YtError("Command {0} is not supported".format(command_name)))

    request_params.update_request_params_mutation_id(params)

    input_stream = convert_to_stream(data)

    output_stream = None
    if command.output_type() != b"Null":
        if "output_format" not in params and command.output_type() != b"Binary":
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
                output_stream = driver_bindings.BufferedStream(size=get_config(client)["read_buffer_size"])

    request_id = generate_uuid(get_option("_random_generator", client))

    logger.debug("Executing command %s with parameters %s and id %s", command_name, repr(params), request_id)

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

    trace_id = params.get("trace_id", None)

    try:
        additional_kwargs = {}
        if "trace_id" in inspect.signature(driver_bindings.Request).parameters:
            additional_kwargs["trace_id"] = trace_id

        request = driver_bindings.Request(
            command_name=command_name,
            parameters=params,
            input_stream=input_stream,
            output_stream=output_stream,
            user=driver_user_name,
            token=token,
            service_ticket=service_ticket,
            **additional_kwargs)
    except TypeError:
        request = driver_bindings.Request(
            command_name=command_name,
            parameters=params,
            input_stream=input_stream,
            output_stream=output_stream,
            user=driver_user_name)

    if get_config(client)["enable_passing_request_id_to_driver"]:
        request.id = request_params.request_id

    response = RpcRequestRetrier(
        driver=driver,
        request=request,
        return_content=return_content,
        make_retries=request_params.allow_retries,
        retry_action=request_params.get_retry_action_rpc(),
        retry_config=retry_config,
        client=client,
    ).run()

    if return_content:
        if output_stream is not None and not isinstance(output_stream, NullStream):
            value = output_stream.getvalue()
            return value
    else:
        def process_error(request):
            if response.is_set() and not response.is_ok():
                raise create_response_error(response.error())

        return ResponseStream(
            lambda: response,
            chunk_iter(output_stream, response, get_config(client)["read_buffer_size"]),
            lambda from_delete: None,
            process_error,
            lambda: response.response_parameters())
