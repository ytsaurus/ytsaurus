from yt.orm.client.common import (
    format_grpc_request,
    format_grpc_response,
    hide_token,
    hexify,
    load_certificate,
    try_close,
)

from yt.orm.library.common import (
    ClientError,
    IncorrectResponseError,
    LazyLogField,
    get_master_instance_tag,
    make_grpc_error,
    milliseconds_to_seconds,
    protobuf_to_dict,
    underscore_case_to_camel_case,
)

try:
    from yt.packages.six import (
        PY3,
        binary_type,
        iteritems,
        itervalues,
    )
except ImportError:
    from six import (
        PY3,
        binary_type,
        iteritems,
        itervalues,
    )

import yt.packages.requests as requests
import yt.yson as yson

from yt.wrapper.common import update, generate_uuid
from yt_proto.yt.core.misc.proto.error_pb2 import TError

from abc import ABCMeta, abstractmethod

import grpc
import json


class _TransportLayer(object):
    __metaclass__ = ABCMeta

    def __init__(
        self,
        balancer_address,
        balancer_ip6_address,
        default_address_suffix,
        config,
        master_discovery,
        logger,
        make_yt_response_error,
        grpc_services,
    ):
        self._balancer_address = self._prepare_address(balancer_address, default_address_suffix)
        self._balancer_ip6_address = balancer_ip6_address
        self._config = update(self._get_default_config(), config)
        self._master_discovery = master_discovery
        self._logger = logger
        self._make_yt_response_error = make_yt_response_error
        self._grpc_services = grpc_services

        self._validate_configuration()

    def update_user_ticket(self, user_ticket):
        self._config["user_ticket"] = user_ticket

    def execute_request(self, service_name, method_name, request, allow_discovery):
        transaction_id = None
        if hasattr(request, "transaction_id") and request.HasField("transaction_id"):
            transaction_id = request.transaction_id
        if hasattr(request, "timestamp_by_transaction_id") and request.HasField("timestamp_by_transaction_id"):
            assert not transaction_id, "Parameter transaction_id and timestamp_by_transaction_id cannot be both specified"
            transaction_id = request.timestamp_by_transaction_id
        stub = self._get_stub(service_name, transaction_id, allow_discovery)
        return self._do_execute_request(stub, method_name, request)

    @abstractmethod
    def _get_transport(self):
        raise NotImplementedError

    @abstractmethod
    def _get_default_config(self):
        raise NotImplementedError

    @abstractmethod
    def _validate_configuration(self):
        raise NotImplementedError

    @abstractmethod
    def _get_stub(self, service_name, transaction_id, allow_discovery):
        raise NotImplementedError

    @abstractmethod
    def _do_execute_request(self, stub, method_name, request):
        raise NotImplementedError

    def _is_secure(self):
        return self._config.get("enable_ssl", True)

    def _prepare_address(self, balancer_address, default_address_suffix):
        for token in (".", ":", "localhost"):
            if token in balancer_address:
                return balancer_address

        return balancer_address + default_address_suffix

    def _get_master_instance_address(self, transaction_id, allow_discovery):
        if transaction_id is None:
            if allow_discovery and self._config.get("enable_master_discovery", True):
                return self._master_discovery.get_random_instance_address(self._get_transport())
            else:
                return self._balancer_address
        else:
            return self._master_discovery.get_instance_address_by_tag(
                get_master_instance_tag(transaction_id),
                self._get_transport(),
            )


class _GrpcLayer(_TransportLayer):
    def __init__(self, *args, **kwargs):
        super(_GrpcLayer, self).__init__(*args, **kwargs)
        self._channels = {}

    def _get_transport(self):
        return "grpc"

    def _get_default_config(self):
        return dict(
            grpc_channel_options=dict(
                max_receive_message_length=32 * (2**20),
                max_metadata_size=1 * (2**20),
            ),
        )

    def _validate_configuration(self):
        if "connect_timeout" in self._config:
            # Underlying Grpc python interface does not provide connection timeout option.
            self._logger.warning(
                "Parameter 'connect_timeout' is not supported for Grpc transport, "
                "use config parameter 'grpc_channel_options' instead",
            )

    def _create_channel_options(self):
        def sanitize_value(value):
            # Try to convert long to int, because Grpc does not support long values.
            # Conversion will take any affect only if it is possible, so it is merely a best effort.
            # This conversion is essential for options passed via command line.
            if not PY3 and isinstance(value, long):  # noqa
                value = int(value)
            return value

        options_mapping = dict()

        def add_option(key, value):
            options_mapping["grpc." + key] = sanitize_value(value)

        def dump_options():
            return tuple(iteritems(options_mapping))

        request_timeout = self._config.get("request_timeout")
        if request_timeout is not None:
            add_option("keepalive_time_ms", int(request_timeout / 2))
            add_option("keepalive_timeout_ms", int(request_timeout))

        if self._is_secure() and self._master_discovery.uses_ip6_addresses():
            add_option("ssl_target_name_override", self._balancer_address)

        config_channel_options = self._config.get("grpc_channel_options")
        if config_channel_options is not None:
            for option, value in iteritems(config_channel_options):
                add_option(option, value)

        return dump_options()

    def _create_channel(self, address):
        options = self._create_channel_options()
        self._logger.debug("Setup Grpc channel options: %s", options)
        if self._is_secure():
            root_certificate = self._config.get("root_certificate", {})
            if "value" in root_certificate:
                credentials_data = root_certificate["value"]
            else:
                credentials_data = load_certificate(root_certificate.get("file_name"))
            if not isinstance(credentials_data, binary_type):
                try:
                    credentials_data = credentials_data.encode("ascii")
                except UnicodeDecodeError:
                    self._logger.exception("Credentials must be bytes string or ascii decodable")
                    raise
            credentials = grpc.ssl_channel_credentials(credentials_data)
            return grpc.secure_channel(address, credentials=credentials, options=options)
        else:
            return grpc.insecure_channel(address, options=options)

    def _get_channel(self, instance_address):
        if instance_address not in self._channels:
            self._channels[instance_address] = self._create_channel(instance_address)
        return self._channels[instance_address]

    def _get_stub(self, service_name, transaction_id, allow_discovery):
        address = self._get_master_instance_address(transaction_id, allow_discovery)
        return self._grpc_services.create_stub(
            service_name,
            self._get_channel(address),
        )

    def _process_response(self, rsp_future, method_name, request_id):
        for item in rsp_future.initial_metadata() + rsp_future.trailing_metadata():
            if item[0] == "yt-error-bin":
                error = TError()
                error.ParseFromString(item[1])
                self._logger.debug(
                    "Request attempt failed (Method: %s, RequestId: %s, Error: %s)",
                    method_name,
                    request_id,
                    hexify(yson.dumps(protobuf_to_dict(error), yson_format="text")),
                )
                raise self._make_yt_response_error(protobuf_to_dict(error), method_name, request_id)
        return rsp_future.result()

    def _do_execute_request(self, stub, method_name, request):
        method = getattr(stub, underscore_case_to_camel_case(method_name))
        request_id = generate_uuid()
        body_log_size_limit = self._config.get("body_log_size_limit", 512)

        metadata = [
            ("yt-request-id", request_id),
        ]

        user = self._config.get("user")
        if user is not None:
            metadata.append(("yt-user", str(user)))
        user_tag = self._config.get("user_tag", None)
        if user_tag is not None:
            metadata.append(("yt-user-tag", str(user_tag)))

        token = self._config.get("token")
        if token is not None:
            metadata.append(("yt-auth-token", str(token)))
        user_ticket = self._config.get("user_ticket")
        if user_ticket is not None:
            metadata.append(("yt-auth-user-ticket", str(user_ticket)))

        request_timeout = self._config.get("request_timeout")
        self._logger.debug(
            "Request attempt started (Method: %s, Target: %s, RequestId: %s, Timeout: %s, Metadata: %s, RequestBody: %s)",
            method_name,
            method._channel.target(),
            request_id,
            request_timeout,
            str(hide_token(metadata)),
            LazyLogField(lambda: format_grpc_request(request, body_log_size_limit)),
        )

        try:
            response_future = method.future(
                request,
                timeout=milliseconds_to_seconds(request_timeout),
                metadata=metadata,
            )

            while True:
                # NB: this hack is used to support SIGINT handling if request hanged up.
                # Some technical details:
                # 1. There is a grpc-thread that handling communication with server.
                #    This thread hanges up on uninterruptible call of epoll_pwait.
                # 2. Main thread goes to call initial_metadata() that hangs on Condition.wait.
                #    But Condition.wait ignores EINTR return code in infinite cycle.
                # To overcome problem we perform manual waiting of condition with limited timeout.
                with response_future._state.condition:
                    if response_future._state.code is not None:
                        break
                    response_future._state.condition.wait(0.1)

            response = self._process_response(response_future, method_name, request_id)
        except grpc.RpcError as exception:
            self._logger.exception(
                "Request attempt failed (Method: %s, Target: %s, RequestId: %s)",
                method_name,
                method._channel.target(),
                request_id,
            )
            raise make_grpc_error(exception)

        self._logger.debug(
            "Request completed (Method: %s, Target: %s, RequestId: %s, ResponseSize: %d, ResponseBody: %s)",
            method_name,
            method._channel.target(),
            request_id,
            response.ByteSize(),
            LazyLogField(lambda: format_grpc_response(response, body_log_size_limit)),
        )

        return response

    # NB! It is needed to explicitly call 'close' method because Grpc channel is not guaranteed
    #     to have destructor releasing all used resources (e.g. memory). See comment from source repository:
    #     https://github.com/grpc/grpc/commit/bccd32dafa1ed60e745c958a55960cf75c56d7d2#diff-e87a8cfa4c9a2b7758e5ab8b8cbbf5d7R927.
    def close(self):
        for channel in itervalues(self._channels):
            try_close(channel)


class _HttpLayer(_TransportLayer):
    class SessionHolder(object):
        def __init__(self, session):
            if session is None:
                self._owned = True
                self._session = requests.Session()
            else:
                self._owned = False
                self._session = session
                if not isinstance(self._session, requests.Session):
                    raise ClientError(
                        "Provided Http session is not an instance of requests.Session subclass"
                    )

        def get(self):
            return self._session

        def close(self):
            if self._owned:
                self._session.close()

    def __init__(self, http_session, *args, **kwargs):
        self._session = self.SessionHolder(http_session)
        super(_HttpLayer, self).__init__(*args, **kwargs)

    def _get_transport(self):
        return "http"

    def _get_default_config(self):
        return dict()

    def _validate_configuration(self):
        if self._is_secure() and self._master_discovery.uses_ip6_addresses():
            raise ClientError("Secure connection to IP6 address is not yet supported")

    def _get_stub(self, service_name, transaction_id, allow_discovery):
        return dict(
            service_name=service_name,
            address=self._get_master_instance_address(transaction_id, allow_discovery),
        )

    def _raise_for_status(self, http_response, method_name, request_id):
        if str(http_response.status_code).startswith("2"):
            return

        self._logger.debug(
            "Bad HTTP status code for request (Method: %s, RequestId: %s, StatusCode: %s)",
            method_name,
            request_id,
            http_response.status_code,
        )

        if "X-YT-Error" in http_response.headers:
            try:
                yt_error = json.loads(http_response.headers["X-YT-Error"])
            except ValueError as exception:
                error = IncorrectResponseError(
                    "Error occurred while parsing X-YT-Error header", inner_errors=[exception]
                )
                error.attributes.update(
                    method=method_name, request_id=request_id, status_code=http_response.status_code
                )
                raise error

            raise self._make_yt_response_error(yt_error, method_name, request_id)

        error = IncorrectResponseError("Bad HTTP status code without specified X-YT-Error header")
        error.attributes.update(
            method=method_name, request_id=request_id, status_code=http_response.status_code
        )
        raise error

    def _timeout_milliseconds_to_seconds(self, timeout):
        if isinstance(timeout, tuple):
            assert len(timeout) == 2
            return tuple(milliseconds_to_seconds(t) for t in timeout)
        return milliseconds_to_seconds(timeout)

    def _do_execute_request(self, stub, method_name, request):
        request_id = generate_uuid()

        if "connect_timeout" in self._config:
            timeout = (self._config["connect_timeout"], self._config["request_timeout"])
        else:
            timeout = self._config.get("request_timeout")

        headers = {
            "Accept": "application/x-protobuf",
            "Content-Type": "application/x-protobuf",
            "X-YT-Request-Id": request_id,
        }

        user = self._config.get("user")
        if user is not None:
            headers["X-YT-User"] = user
        user_tag = self._config.get("user_tag", None)
        if user_tag is not None:
            headers["X-YT-User-Tag"] = user_tag
        token = self._config.get("token")
        if token is not None:
            headers["Authorization"] = "OAuth " + token
        user_ticket = self._config.get("user_ticket")
        if user_ticket is not None:
            headers["X-Ya-User-Ticket"] = user_ticket

        # The "X-Request-Timeout" header sets the server-side timeout in milliseconds.
        # We usually set it to the same timeout as our client-side.
        # Separate "x_request_timeout" flag is primarily for tests.
        if "x_request_timeout" in self._config:
            headers["X-Request-Timeout"] = str(self._config["x_request_timeout"])
        elif "request_timeout" in self._config:
            headers["X-Request-Timeout"] = str(self._config["request_timeout"])

        body_log_size_limit = self._config.get("body_log_size_limit", 512)
        method_path = (
            underscore_case_to_camel_case(stub["service_name"])
            + "/"
            + underscore_case_to_camel_case(method_name)
        )
        protocol = ("https" if self._is_secure() else "http")
        url = "{}://{}/{}".format(protocol, stub["address"], method_path)
        self._logger.debug(
            "Request attempt started (Method: %s, Url: %s, Timeout: %s, Headers: %s, RequestBody: %s)",
            method_name,
            url,
            timeout,
            str(hide_token(headers)),
            LazyLogField(lambda: format_grpc_request(request, body_log_size_limit)),
        )

        root_certificate = self._config.get("root_certificate", {}).get("file_name")
        try:
            http_response = self._session.get().post(
                url,
                data=request.SerializeToString(),
                headers=headers,
                timeout=self._timeout_milliseconds_to_seconds(timeout),
                verify=root_certificate,
            )
        except Exception:
            self._logger.exception(
                "Request attempt failed (Method: %s, RequestId: %s)",
                method_name,
                request_id,
            )
            raise

        self._raise_for_status(http_response, method_name, request_id)

        deserializer = getattr(
            self._grpc_services.create_fake_channel_stub(stub["service_name"], self._logger),
            underscore_case_to_camel_case(method_name),
        )._response_deserializer
        response = deserializer(http_response.content)

        self._logger.debug(
            "Request completed (Method: %s, RequestId: %s, ResponseSize: %d, ResponseBody: %s)",
            method_name,
            request_id,
            len(http_response.content),
            LazyLogField(lambda: format_grpc_response(response, body_log_size_limit)),
        )

        return response

    def close(self):
        self._session.close()


def make_transport_layer(transport, *args, **kwargs):
    http_session = kwargs.pop("http_session", None)

    if transport == "grpc":
        return _GrpcLayer(*args, **kwargs)
    elif transport == "http":
        return _HttpLayer(http_session, *args, **kwargs)
    else:
        raise ClientError("Unknown transport '{}'".format(transport))
