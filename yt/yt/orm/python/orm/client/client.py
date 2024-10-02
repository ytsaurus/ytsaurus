from abc import ABCMeta, abstractmethod, abstractproperty

from yt.orm.client.transport_layer import make_transport_layer
from yt.orm.client.common import (
    try_close,
)
from yt.orm.library.common import (
    ClientError,
    ContinuationTokenVersionMismatchError,
    GrpcResourceExhaustedError,
    camel_case_to_underscore_case,
    dict_timestamp_to_microseconds,
    dict_to_protobuf,
    get_retriable_errors,
    make_yt_response_error,
    microseconds_to_proto_timestamp,
    proto_timestamp_to_microseconds,
    protobuf_to_dict,
)

from yt.orm.library.retries import get_default_retries_config, get_retrier_factory

from yt_proto.yt.orm.client.proto import object_pb2

from yt_yson_bindings import loads_proto, dumps_proto

import yt.yson as yson

from yt.common import update

try:
    from yt.packages.six import string_types
    from yt.packages.six.moves import xrange
except ImportError:
    from six import string_types
    from six.moves import xrange

import grpc

from google.protobuf import message as pb_message

from copy import deepcopy
import os
import types


_DEFAULT_CONFIG = dict(
    request_timeout=1000 * 60 * 2,
    master_discovery_expiration_time=1000 * 60 * 2,
)

_SENTINEL = object()


def _get_token_by_ssh_session(logger, oauth_client_id, oauth_client_secret):
    try:
        import library.python.oauth as lpo
    except ImportError:
        logger.exception(
            "Module library.python.oauth not found, cannot receive token by ssh session"
        )
        return

    config = {
        "oauth_client_id": oauth_client_id,
        "oauth_client_secret": oauth_client_secret,
    }

    try:
        token = lpo.get_token(config["oauth_client_id"], config["oauth_client_secret"])
        if not token:
            logger.error(
                "Received an empty token using current ssh session, "
                "maybe ssh forwarding (see `ssh -A` option) of the session is disabled"
            )
            return None
    except Exception:
        logger.exception("Failed to receive token using current ssh session")
        return None

    return token


def find_token(
    logger,
    env_name,
    dir_name,
    oauth_client_id,
    oauth_client_secret,
    allow_receive_token_by_ssh_session=True,
):
    token = os.environ.get(env_name)
    if token is not None:
        return token
    if dir_name:
        token_path = os.path.join(os.path.expanduser("~"), dir_name, "token")
        if os.path.isfile(token_path):
            with open(token_path, "r") as token_file:
                token = token_file.read().strip()
            return token
    if allow_receive_token_by_ssh_session:
        return _get_token_by_ssh_session(logger, oauth_client_id, oauth_client_secret)
    return None


def get_proto_enum_value_name(proto_enum_value):
    for field in proto_enum_value.GetOptions().ListFields():
        if field[0].name == "enum_value_name":
            return field[1]
    return None


def to_proto_enum(proto_class, value):
    descriptor = getattr(proto_class, "DESCRIPTOR")
    for proto_value in descriptor.values:
        if get_proto_enum_value_name(proto_value) == value:
            return proto_value
    raise ClientError(
        "`{}` is not a valid value for enum {}".format(
            value,
            proto_class._enum_type.full_name,
        ),
    )


def to_proto_enum_by_number(proto_class, number):
    descriptor = getattr(proto_class, "DESCRIPTOR")
    for proto_value in descriptor.values:
        if proto_value.number == number:
            return proto_value
    raise ClientError(
        "Number `{}` is not a valid value for enum {}".format(
            number,
            proto_class._enum_type.full_name,
        ),
    )


def _prepare_enum(value, type):
    if isinstance(value, str):
        value = to_proto_enum(type, value).number
    return value


def _call_and_raise_if_no_outer_exception(
    description, callback, logger, exc_type, exc_value, traceback
):
    has_outer_exception = not (exc_type is None and exc_value is None and traceback is None)
    try:
        callback()
    except:  # noqa
        # Do not shadow outer exception.
        if has_outer_exception:
            logger.exception("Could not %s", description)
        else:
            raise


class GrpcServices(object):
    def __init__(self):
        self._services = {}

    def add_service(self, service_name, stub_type):
        assert service_name not in self._services, "Service {} already registered".format(service_name)
        self._services[service_name] = {
            "stub_type": stub_type,
        }

    def create_stub(self, service_name, channel):
        assert service_name in self._services, "Unknown service_name {}".format(service_name)
        return self._services[service_name]["stub_type"](channel)

    def create_fake_channel_stub(self, service_name, logger):
        class ChannelWrapper(object):
            def __init__(self, channel, logger):
                self._channel = channel
                self._logger = logger

            def __enter__(self):
                return self._channel

            def __exit__(self, *args, **kwargs):
                _call_and_raise_if_no_outer_exception(
                    "close fake channel",
                    lambda: try_close(self._channel),
                    self._logger,
                    *args,
                    **kwargs
                )

        with ChannelWrapper(grpc.insecure_channel("fake"), logger) as fake_channel:
            return self.create_stub(service_name, fake_channel)


class BatchingOptions(object):
    DEFAULT_BATCH_SIZE = 1000

    DEFAULT_MAX_BATCH_SIZE = 10000

    DEFAULT_INCREASING_ADDITIVE = 10
    DEFAULT_DECREASING_MULTIPLIER = 2

    DEFAULT_ATTEMPT_COUNT = 1

    MIN_BATCH_SIZE = 1

    # If batching is required, a method must either fail or be executed
    # using batching. Otherwise a method is force to use batching
    # only if it is supported for the given set of call options.
    def __init__(
        self,
        batch_size=DEFAULT_BATCH_SIZE,
        attempt_count=DEFAULT_ATTEMPT_COUNT,
        max_batch_size=DEFAULT_MAX_BATCH_SIZE,
        required=True,
    ):
        self.current_batch_size = min(max(self.MIN_BATCH_SIZE, batch_size), max_batch_size)
        self.attempt_count = attempt_count
        self.max_batch_size = max_batch_size
        self.required = required

    def increase_batch_size(self):
        self.current_batch_size += self.DEFAULT_INCREASING_ADDITIVE
        self.current_batch_size = min(self.current_batch_size, self.max_batch_size)
        return self.current_batch_size

    def decrease_batch_size(self):
        self.current_batch_size //= self.DEFAULT_DECREASING_MULTIPLIER
        self.current_batch_size = max(self.current_batch_size, self.MIN_BATCH_SIZE)
        return self.current_batch_size


class ClientBase(object):
    __metaclass__ = ABCMeta

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        _call_and_raise_if_no_outer_exception(
            "close client", lambda: self.close(), self.get_logger(), *args, **kwargs
        )

    def __init__(
        self,
        address,
        ip6_address=None,
        transport=None,
        config=None,
        grpc_services=None,
        http_session=None,
    ):
        self._address = address
        self._ip6_address = ip6_address
        self._transport = transport if transport is not None else "grpc"
        self._config = update(_DEFAULT_CONFIG, config)
        self._grpc_services = grpc_services
        self._http_session = http_session

        if self._http_session is not None:
            assert self._transport == "http"

        self._transport_layer = self.make_transport_layer()

    @property
    def address(self):
        return self._address

    @abstractmethod
    def get_logger(self):
        raise NotImplementedError

    def get_retriable_methods(self):
        return list()

    def get_user_id(self):
        return self._config.get("user")

    def _get_method_description(self, method_name):
        return "{} /{} request".format(self._transport.upper(), method_name)

    def _get_retries_config(self, enabled):
        config = self._config.get("retries", get_default_retries_config())
        if not enabled:
            config = update(config, dict(enable=False))
        return config

    def _execute_request(
        self,
        service_name,
        method_name,
        request,
        allow_discovery=True,
        allow_retries=True,
    ):
        retriable_methods = self.get_retriable_methods()
        retries_enabled = allow_retries and (method_name in retriable_methods)
        retrier_factory = get_retrier_factory(
            self.get_logger(),
            self._get_method_description(method_name),
            get_retriable_errors(),
            config=self._get_retries_config(retries_enabled),
            request_timeout=self._config.get("request_timeout", None),
        )
        return retrier_factory(
            lambda: self._transport_layer.execute_request(service_name, method_name, request, allow_discovery)
        ).run()

    def close(self):
        self._transport_layer.close()


class OrmClientTraits(object):
    def __init__(self, object_type_enum_type, access_control_permission_enum_type, access_control_action_enum_type):
        self.object_type_enum_type = object_type_enum_type
        self.access_control_permission_enum_type = access_control_permission_enum_type
        self.access_control_action_enum_type = access_control_action_enum_type

    def prepare_object_type(self, object_type):
        return _prepare_enum(object_type, self.object_type_enum_type)

    def prepare_access_control_permission(self, permission):
        return _prepare_enum(permission, self.access_control_permission_enum_type)

    def prepare_access_control_action(self, action):
        return _prepare_enum(action, self.access_control_permission_action_type)


# Use OrmClient as a context manager or call OrmClient.close at the end (see _GrpcLayer.close for details).
class OrmClient(ClientBase):
    __metaclass__ = ABCMeta

    def get_yt_response_error_maker(self):
        return make_yt_response_error

    @abstractproperty
    def client_traits(self):  # type: () -> OrmClientTraits
        raise NotImplementedError

    @abstractmethod
    def get_master_discovery(self):
        raise NotImplementedError

    @abstractmethod
    def get_discovery_service_pb2(self):
        raise NotImplementedError

    @abstractmethod
    def get_object_service_pb2(self):
        return NotImplementedError

    @abstractmethod
    def get_grpc_default_address_suffix(self):
        return NotImplementedError

    @abstractmethod
    def get_http_default_address_suffix(self):
        return NotImplementedError

    @abstractmethod
    def get_select_object_history_index_mode(self):
        raise NotImplementedError

    def get_lock_type(self):
        return object_pb2.ELockType

    def get_aggregate_mode(self):
        return object_pb2.EAggregateMode

    def make_transport_layer(self):
        return make_transport_layer(
            self._transport,
            self._address,
            self._ip6_address,
            self._get_default_address_suffix(),
            self._config,
            self.get_master_discovery(),
            self.get_logger(),
            self.get_yt_response_error_maker(),
            self._grpc_services,
            http_session=self._http_session,
        )

    def _get_default_address_suffix(self):
        if self._transport == "grpc":
            return self.get_grpc_default_address_suffix()
        elif self._transport == "http":
            return self.get_http_default_address_suffix()
        else:
            raise ClientError("Unknown transport '{}'".format(self._transport))

    def update_user_ticket(self, user_ticket):
        self._config["user_ticket"] = user_ticket
        self._transport_layer.update_user_ticket(user_ticket)

    def get_retriable_methods(self):
        return (
            "aggregate_objects",
            "generate_timestamp",
            "get_masters",
            "get_object",
            "get_objects",
            "get_user_access_allowed_to",
            "select_object_history",
            "select_objects",
            "start_transaction",
            "watch_objects",
        )

    def _set_selector_impl(self, req_selector, selectors):
        del req_selector.paths[:]
        for selector in selectors:
            if isinstance(selector, string_types):
                req_selector.paths.append(selector)
            elif isinstance(selector, dict):
                req_selector.paths.append(selector["path"])
            else:
                raise Exception("Selector must be a 'str' or a 'dict' but got {}".format(selector))

    def _set_selector_proto(self, req, selectors):
        if selectors is None:
            selectors = []

        self._set_selector_impl(req.selector, selectors)

    def _set_distinct_by_proto(self, options, selectors):
        if selectors is None:
            return

        self._set_selector_impl(options.distinct_by, selectors)

    def _set_order_by_proto(self, req, order_by_expressions):
        if order_by_expressions is None:
            return
        for order_by_expression in order_by_expressions:
            if isinstance(order_by_expression, dict):
                proto_order_by_expression = req.order_by.expressions.add()
                proto_order_by_expression.expression = order_by_expression["expression"]
                if "descending" in order_by_expression:
                    proto_order_by_expression.descending = order_by_expression["descending"]
            else:
                raise Exception(
                    "Order by expressions must be of type 'dict' but got {}".format(
                        order_by_expression
                    )
                )

    def _set_options_proto(self, req, options, proto_type):
        if options is None:
            return

        req.options.CopyFrom(dict_to_protobuf(options, proto_type))

    def _set_get_object_options_proto(self, req, options):
        self._set_options_proto(req, options, self.get_object_service_pb2().TGetObjectOptions)

    def _set_select_objects_options_proto(self, req, options):
        self._set_options_proto(req, options, self.get_object_service_pb2().TSelectObjectsOptions)

    def _set_common_options_proto(self, req, options):
        if options is not None:
            req.common_options.CopyFrom(
                dict_to_protobuf(options, self.get_object_service_pb2().TCommonRequestOptions)
            )

    def _set_filter_proto(self, req, filter):
        if filter is not None:
            req.filter.query = filter

    def _prepare_lock_type(self, value):
        return _prepare_enum(value, self.get_lock_type())

    def _prepare_index_mode(self, value):
        return _prepare_enum(value, self.get_select_object_history_index_mode())

    def _prepare_aggregate_mode(self, value):
        return _prepare_enum(value, self.get_aggregate_mode())

    def _parse_payload_value(self, proto_payload, selector):
        value = None
        if proto_payload.HasField("yson"):
            yson_value = proto_payload.yson
            if isinstance(selector, dict) and "proto_class" in selector:
                proto_class = selector["proto_class"]
                value = loads_proto(yson_value, proto_class, skip_unknown_fields=True)
            else:
                value = yson.loads(yson_value)
        return value

    def _parse_attribute_list_values(self, proto_result, selectors, enable_structured_response):
        if proto_result is None:
            return None

        items = []
        payloads = proto_result.value_payloads
        timestamps = proto_result.timestamps
        if enable_structured_response:
            # Iteration till the maximum of payload and timestamp sizes handles two corner cases at once:
            # - fetch_root_object = true always returns one payload no matter how many of them have been requested;
            # - fetch_timestamps = false returns no timestamp at all.
            for index in xrange(max(len(payloads), len(timestamps))):
                item = dict()
                if index < len(payloads):
                    item["value"] = self._parse_payload_value(
                        payloads[index],
                        selectors[index] if index < len(selectors) else "")
                if index < len(timestamps):
                    item["timestamp"] = timestamps[index]
                items.append(item)
        else:
            for index in xrange(len(payloads)):
                items.append(self._parse_payload_value(payloads[index], selectors[index]))
        return items

    def _get_object_identity(self, subrequest):
        if "meta" in subrequest:
            return subrequest["meta"]
        if "object_identity" in subrequest:
            return subrequest["object_identity"]
        if "object_id" in subrequest:  # COMPAT
            return subrequest["object_id"]

    def _fill_object_identity(self, req, object_identity):
        if isinstance(object_identity, string_types):
            if len(object_identity) > 0 and object_identity[0] == "{":
                req.meta.yson = object_identity
            else:
                req.meta.yson = yson.dumps({"key": object_identity})
        elif isinstance(object_identity, yson.yson_types.YsonStringProxy):
            bytes = yson.get_bytes(object_identity)
            if len(bytes) > 0 and bytes[0] == b"{":
                req.meta.yson = object_identity
            else:
                req.meta.yson = yson.dumps({"key": object_identity})
        elif isinstance(object_identity, dict):
            req.meta.yson = yson.dumps(object_identity)
        else:
            raise Exception("Unexpected object identity type {}".format(type(object_identity)))

    def _fill_set_update_requests(self, proto_set_updates, set_updates):
        for set_update in set_updates:
            proto_update = proto_set_updates.add()
            proto_update.path = set_update["path"]
            if "value" in set_update:
                value = set_update["value"]
                if isinstance(value, pb_message.Message):
                    proto_update.value_payload.yson = dumps_proto(value)
                else:
                    proto_update.value_payload.yson = yson.dumps(value)
            proto_update.recursive = set_update.get("recursive", False)
            if "shared_write" in set_update:
                proto_update.shared_write = set_update["shared_write"]
            proto_update.aggregate_mode = self._prepare_aggregate_mode(set_update.get("aggregate_mode", "unspecified"))

    def _fill_set_root_update_requests(self, proto_set_root_updates, set_root_updates):
        for set_root_update in set_root_updates:
            proto_update = proto_set_root_updates.add()
            proto_update.paths.extend(set_root_update["paths"])
            if "value" in set_root_update:
                value = set_root_update["value"]
                if isinstance(value, pb_message.Message):
                    proto_update.value_payload.yson = dumps_proto(value)
                else:
                    proto_update.value_payload.yson = yson.dumps(value)
            proto_update.recursive = set_root_update.get("recursive", False)
            if "shared_write" in set_root_update:
                proto_update.shared_write = set_root_update["shared_write"]
            proto_update.aggregate_mode = self._prepare_aggregate_mode(
                set_root_update.get("aggregate_mode", "unspecified")
            )

    def _fill_remove_update_requests(self, proto_remove_updates, remove_updates):
        for remove_update in remove_updates:
            proto_update = proto_remove_updates.add()
            proto_update.path = remove_update["path"]
            proto_update.force = remove_update.get("force", False)

    def _fill_lock_update_requests(self, proto_lock_updates, lock_updates):
        for lock_update in lock_updates:
            proto_update = proto_lock_updates.add()
            proto_update.path = lock_update["path"]
            proto_update.lock_type = self._prepare_lock_type(lock_update["lock_type"])

    def _fill_method_calls_requests(self, proto_method_calls, method_calls):
        for method in method_calls:
            proto_update = proto_method_calls.add()
            proto_update.path = method["path"]
            if "value" in method:
                value = method["value"]
                if isinstance(value, pb_message.Message):
                    proto_update.value_payload.yson = dumps_proto(value)
                else:
                    proto_update.value_payload.yson = yson.dumps(value)

    def _fill_attribute_timestamp_prerequisites(
        self, proto_attribute_timestamp_prerequisites, attribute_timestamp_prerequisites
    ):
        for prerequisite in attribute_timestamp_prerequisites:
            proto_prerequisite = proto_attribute_timestamp_prerequisites.add()
            proto_prerequisite.path = prerequisite["path"]
            proto_prerequisite.timestamp = prerequisite["timestamp"]

    def _create_grpc_stub_wrapper(self, service_name):
        def create_proto_method_wrapper(method_name):
            return lambda obj, request, **kwargs: self._execute_request(
                service_name=service_name,
                method_name=camel_case_to_underscore_case(method_name),
                request=request,
            )

        stub = self._grpc_services.create_fake_channel_stub(
            service_name,
            self.get_logger(),
        )
        new_stub_class = type(stub.__class__.__name__, (object,), {})
        new_stub = new_stub_class()
        for key in dir(stub):
            # TODO(ignat): make this filtering in more reliable way.
            if key.startswith("__"):
                continue
            setattr(new_stub, key, types.MethodType(create_proto_method_wrapper(key), new_stub))
        return new_stub

    def create_grpc_object_stub(self):
        return self._create_grpc_stub_wrapper("object_service")

    def create_grpc_discovery_stub(self):
        return self._create_grpc_stub_wrapper("discovery_service")

    def get_object(
        self,
        object_type,
        object_identity=_SENTINEL,
        selectors=_SENTINEL,
        timestamp=None,
        options=None,
        enable_structured_response=False,
        common_options=None,
        object_id=None,  # COMPAT
        timestamp_by_transaction_id=None,
        transaction_id=None,
    ):
        # COMPAT this makes object_id an alias for object_identity. This will provide a migration
        # path for callers that use named arguments.
        if object_id is not None:
            assert object_identity is _SENTINEL
            object_identity = object_id
        assert object_identity is not _SENTINEL
        assert selectors is not _SENTINEL

        proto_req = self.get_object_service_pb2().TReqGetObject()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        self._fill_object_identity(proto_req, object_identity)
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        self._set_selector_proto(proto_req, selectors)
        self._set_get_object_options_proto(proto_req, options)
        if timestamp is not None:
            proto_req.timestamp = timestamp
        if timestamp_by_transaction_id is not None:
            proto_req.timestamp_by_transaction_id = timestamp_by_transaction_id
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        self._set_common_options_proto(proto_req, common_options)

        proto_rsp = self._execute_request("object_service", "get_object", proto_req)

        result = self._parse_attribute_list_values(
            proto_rsp.result if proto_rsp.HasField("result") else None,
            selectors,
            enable_structured_response,
        )

        if enable_structured_response:
            rsp = dict()
            rsp["result"] = result
            if proto_rsp.HasField("timestamp"):
                rsp["timestamp"] = proto_rsp.timestamp
            if proto_rsp.HasField("performance_statistics"):
                rsp["performance_statistics"] = protobuf_to_dict(
                    proto_rsp.performance_statistics,
                )
            return rsp
        else:
            return result

    def get_objects(
        self,
        object_type,
        object_identities,
        selectors,
        timestamp=None,
        options=None,
        enable_structured_response=False,
        common_options=None,
        timestamp_by_transaction_id=None,
        transaction_id=None,
    ):
        proto_req = self.get_object_service_pb2().TReqGetObjects()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        for object_identity in object_identities:
            proto_subreq = proto_req.subrequests.add()
            self._fill_object_identity(proto_subreq, object_identity)
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        self._set_selector_proto(proto_req, selectors)
        self._set_get_object_options_proto(proto_req, options)
        if timestamp is not None:
            proto_req.timestamp = timestamp
        if timestamp_by_transaction_id is not None:
            proto_req.timestamp_by_transaction_id = timestamp_by_transaction_id
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id

        self._set_common_options_proto(proto_req, common_options)
        proto_rsp = self._execute_request("object_service", "get_objects", proto_req)

        results = [
            self._parse_attribute_list_values(
                proto_subrsp.result if proto_subrsp.HasField("result") else None,
                selectors,
                enable_structured_response,
            )
            for proto_subrsp in proto_rsp.subresponses
        ]

        if enable_structured_response:
            rsp = dict()
            rsp["results"] = results
            if proto_rsp.HasField("timestamp"):
                rsp["timestamp"] = proto_rsp.timestamp
            if proto_rsp.HasField("performance_statistics"):
                rsp["performance_statistics"] = protobuf_to_dict(
                    proto_rsp.performance_statistics,
                )
            return rsp
        else:
            return results

    @staticmethod
    def _unpack_arguments(expected_arguments, arguments):
        for key in arguments:
            if key not in expected_arguments:
                raise Exception("Unexpected key {} in arguments".format(key))
        for expected_argument in expected_arguments:
            if expected_argument not in arguments:
                raise Exception("Expected key {} in arguments".format(expected_argument))
        return [arguments[expected_argument] for expected_argument in expected_arguments]

    def _select_objects(self, object_type, **arguments):
        expected_keys = [
            "filter",
            "selectors",
            "timestamp",
            "offset",
            "limit",
            "options",
            "enable_structured_response",
            "order_by",
            "index",
            "common_options",
        ]
        arguments = self._unpack_arguments(expected_keys, arguments)
        (
            filter,
            selectors,
            timestamp,
            offset,
            limit,
            options,
            enable_structured_response,
            order_by,
            index,
            common_options,
        ) = arguments

        proto_req = self.get_object_service_pb2().TReqSelectObjects()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        self._set_select_objects_options_proto(proto_req, options)
        self._set_filter_proto(proto_req, filter)
        self._set_selector_proto(proto_req, selectors)
        self._set_order_by_proto(proto_req, order_by)
        self._set_common_options_proto(proto_req, common_options)
        if timestamp is not None:
            proto_req.timestamp = timestamp
        # COMPAT(babenko): offset and limit are deprecated
        if offset is not None:
            proto_req.offset.value = offset
        if limit is not None:
            proto_req.limit.value = limit
        if index is not None:
            proto_req.index.name = index

        proto_rsp = self._execute_request("object_service", "select_objects", proto_req)

        results = [
            self._parse_attribute_list_values(result, selectors, enable_structured_response)
            for result in proto_rsp.results
        ]

        if enable_structured_response:
            rsp = dict()
            rsp["results"] = results
            if proto_rsp.HasField("timestamp"):
                rsp["timestamp"] = proto_rsp.timestamp
            if proto_rsp.HasField("continuation_token"):
                rsp["continuation_token"] = proto_rsp.continuation_token
            if proto_rsp.HasField("performance_statistics"):
                rsp["performance_statistics"] = protobuf_to_dict(
                    proto_rsp.performance_statistics,
                )
            return rsp
        else:
            return results

    def _batch_select_objects(self, object_type, batching_options, **arguments):
        expected_keys = [
            "filter",
            "selectors",
            "timestamp",
            "options",
            "limit",
            "order_by",
            "index",
            "common_options",
        ]
        arguments = self._unpack_arguments(expected_keys, arguments)
        filter, selectors, timestamp, options, limit, order_by, index, common_options = arguments

        if "continuation_token" in options:
            raise Exception("Unexpected continuation_token in options in batch_select method")

        if "offset" in options:
            raise Exception("Unexpected offset in options in batch_select method")

        if timestamp is None:
            timestamp = self.generate_timestamp()

        continuation_token = None
        limit = options.get("limit", limit)

        while True:
            options["limit"] = batching_options.current_batch_size
            if limit is not None:
                if limit == 0:
                    break
                options["limit"] = min(options["limit"], limit)

            if continuation_token is not None:
                options["continuation_token"] = continuation_token

            try:
                self.get_logger().debug("Selecting %s batch", object_type)
                response = self._select_objects(
                    object_type,
                    filter=filter,
                    selectors=selectors,
                    timestamp=timestamp,
                    options=options,
                    offset=None,
                    limit=None,
                    enable_structured_response=True,
                    order_by=order_by,
                    index=index,
                    common_options=common_options,
                )
            except GrpcResourceExhaustedError:
                if batching_options.current_batch_size == 1:
                    raise
                previous_batch_size = batching_options.current_batch_size
                batching_options.decrease_batch_size()
                self.get_logger().debug(
                    "Decreasing batch size due to excessive output (previous_size: %d, new_size: %d)",
                    previous_batch_size,
                    batching_options.current_batch_size,
                )
                continue

            if "continuation_token" in response:
                continuation_token = response["continuation_token"]

            if not response["results"]:
                break

            if limit is not None:
                limit -= len(response["results"])

            self.get_logger().debug(
                "Selected %s batch (size: %d)", object_type, len(response["results"])
            )

            yield response

            if len(response["results"]) == batching_options.current_batch_size:
                batching_options.increase_batch_size()

    def _batch_select_objects_with_retries(self, object_type, batching_options, **arguments):
        if batching_options.attempt_count <= 0:
            raise Exception(
                "Attempt count must be positive, but got {}".format(batching_options.attempt_count)
            )
        expected_keys = [
            "filter",
            "selectors",
            "timestamp",
            "limit",
            "options",
            "enable_structured_response",
            "order_by",
            "index",
            "common_options",
        ]
        arguments = self._unpack_arguments(expected_keys, arguments)
        (
            filter,
            selectors,
            timestamp,
            limit,
            options,
            enable_structured_response,
            order_by,
            index,
            common_options,
        ) = arguments

        if options is None:
            options = dict()

        results = None
        last_response = None
        for attempt_number in range(batching_options.attempt_count):
            try:
                selected_object_generator = self._batch_select_objects(
                    object_type,
                    deepcopy(batching_options),
                    filter=filter,
                    selectors=selectors,
                    timestamp=timestamp,
                    options=deepcopy(options),
                    limit=limit,
                    order_by=order_by,
                    index=index,
                    common_options=common_options,
                )
                results = []
                for response in selected_object_generator:
                    results.extend(response["results"])
                    if last_response is not None and last_response.get(
                        "timestamp", None
                    ) != response.get("timestamp", None):
                        raise Exception(
                            "Different timestamps in responses. Previous timestamp {}, current timestamp {}".format(
                                last_response.get("timestamp", None),
                                response.get("timestamp", None),
                            )
                        )
                    last_response = response
                break
            except ContinuationTokenVersionMismatchError:
                self.get_logger().exception(
                    "Continuation token versions mismatch on %d attempt", attempt_number
                )
                if attempt_number == batching_options.attempt_count - 1:
                    raise

        if enable_structured_response:
            rsp = dict()
            rsp["results"] = results
            if last_response is not None and "timestamp" in last_response:
                rsp["timestamp"] = last_response["timestamp"]
            if last_response is not None and "continuation_token" in last_response:
                rsp["continuation_token"] = last_response["continuation_token"]
            return rsp
        else:
            return [[field["value"] for field in fields] for fields in results]

    def select_objects(
        self,
        object_type,
        filter=None,
        selectors=None,
        timestamp=None,
        offset=None,
        limit=None,
        options=None,
        enable_structured_response=False,
        batching_options=None,
        order_by=None,
        index=None,
        common_options=None,
    ):
        def select_without_batching():
            return self._select_objects(
                object_type,
                filter=filter,
                selectors=selectors,
                timestamp=timestamp,
                offset=offset,
                limit=limit,
                options=options,
                enable_structured_response=enable_structured_response,
                order_by=order_by,
                index=index,
                common_options=common_options,
            )

        def select_without_batching_or_raise(exception_msg):
            if batching_options is None or not batching_options.required:
                return select_without_batching()
            else:
                raise Exception(exception_msg)

        if batching_options is None:
            return select_without_batching()

        if not isinstance(batching_options, BatchingOptions):
            raise Exception("Incorrect type of batching options {}".format(batching_options))

        if (offset is not None) or (options is not None and "offset" in options):
            return select_without_batching_or_raise(
                "Batching options and offset cannot be both specified"
            )
        if options is not None and "continuation_token" in options:
            return select_without_batching_or_raise(
                "Batching options and continuation token cannot be both specified"
            )

        return self._batch_select_objects_with_retries(
            object_type,
            batching_options,
            filter=filter,
            selectors=selectors,
            timestamp=timestamp,
            limit=limit,
            options=options,
            enable_structured_response=enable_structured_response,
            order_by=order_by,
            index=index,
            common_options=common_options,
        )

    def select_object_history(
        self,
        object_type,
        object_identity,
        selectors,
        options=None,
        common_options=None,
    ):
        proto_req = self.get_object_service_pb2().TReqSelectObjectHistory()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        self._fill_object_identity(proto_req, object_identity)
        self._set_selector_proto(proto_req, selectors)

        if options is not None:
            if "uuid" in options:
                proto_req.options.uuid = options["uuid"]
            if "limit" in options:
                proto_req.options.limit = options["limit"]
            if "continuation_token" in options:
                proto_req.options.continuation_token = options["continuation_token"]
            if "interval" in options:
                begin, end = options["interval"]
                if begin is not None:
                    proto_req.options.interval.begin.CopyFrom(
                        microseconds_to_proto_timestamp(begin)
                    )
                if end is not None:
                    proto_req.options.interval.end.CopyFrom(microseconds_to_proto_timestamp(end))
            if "timestamp_interval" in options:
                begin, end = options["timestamp_interval"]
                if begin is not None:
                    proto_req.options.timestamp_interval.begin = begin
                if end is not None:
                    proto_req.options.timestamp_interval.end = end
            if "descending_time_order" in options:
                proto_req.options.descending_time_order = options["descending_time_order"]
            if "distinct" in options:
                proto_req.options.distinct = options["distinct"]
            if "distinct_by" in options:
                self._set_distinct_by_proto(proto_req.options, options["distinct_by"])
            if "fetch_root_object" in options:
                proto_req.options.fetch_root_object = options["fetch_root_object"]
            if "index_mode" in options:
                proto_req.options.index_mode = self._prepare_index_mode(options["index_mode"])
            if "allow_time_mode_conversion" in options:
                proto_req.options.allow_time_mode_conversion = options["allow_time_mode_conversion"]
            if "filter" in options:
                proto_req.options.filter = options["filter"]
        self._set_common_options_proto(proto_req, common_options)

        proto_rsp = self._execute_request("object_service", "select_object_history", proto_req)

        events = []
        for rsp_event in proto_rsp.events:
            event = dict()
            if rsp_event.HasField("time"):
                event["time"] = proto_timestamp_to_microseconds(rsp_event.time)
            if rsp_event.HasField("timestamp"):
                event["timestamp"] = rsp_event.timestamp
            event["event_type"] = rsp_event.event_type
            event["user"] = rsp_event.user
            event["user_tag"] = rsp_event.user_tag
            event["results"] = (
                self._parse_attribute_list_values(rsp_event.results, selectors, True)
                if rsp_event.HasField("results")
                else None
            )
            # Convert protobuf list of strings to the python native list.
            event["history_enabled_attributes"] = [
                attribute for attribute in rsp_event.history_enabled_attributes
            ]
            if rsp_event.HasField("transaction_context"):
                event["transaction_context"] = protobuf_to_dict(rsp_event.transaction_context)

            events.append(event)

        result = dict()
        result["events"] = events
        result["continuation_token"] = (
            proto_rsp.continuation_token if proto_rsp.HasField("continuation_token") else None
        )
        if proto_rsp.HasField("last_trim_time"):
            result["last_trim_time"] = proto_timestamp_to_microseconds(proto_rsp.last_trim_time)
        if proto_rsp.HasField("performance_statistics"):
            result["performance_statistics"] = protobuf_to_dict(proto_rsp.performance_statistics)

        return result

    def watch_objects(
        self,
        object_type,
        event_count_limit=None,
        continuation_token=None,
        start_timestamp=None,
        timestamp=None,
        time_limit=None,
        read_time_limit=None,
        enable_structured_response=False,
        filter=None,
        selectors=None,
        request_meta_response=False,
        watch_log=None,
        skip_trimmed=False,
        tablets=None,
        start_from_earliest_offset=None,
        common_options=None,
        fetch_changed_attributes=False,
        required_tags=None,
        excluded_tags=None,
    ):
        proto_req = self.get_object_service_pb2().TReqWatchObjects()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        if start_timestamp is not None:
            proto_req.start_timestamp = start_timestamp
        if start_from_earliest_offset is not None:
            proto_req.start_from_earliest_offset = start_from_earliest_offset
        if timestamp is not None:
            proto_req.timestamp = timestamp
        if continuation_token is not None:
            proto_req.continuation_token = continuation_token
        if event_count_limit is not None:
            proto_req.event_count_limit = event_count_limit
        if time_limit is not None:
            proto_req.time_limit.FromTimedelta(time_limit)
        if read_time_limit is not None:
            proto_req.read_time_limit.FromTimedelta(read_time_limit)
        if filter is not None:
            self._set_filter_proto(proto_req, filter)
        if selectors is not None:
            self._set_selector_proto(proto_req, selectors)
        if watch_log is not None:
            proto_req.watch_log = watch_log
        if tablets is not None:
            for tablet in tablets:
                proto_req.tablets.append(tablet)
        if required_tags is not None:
            for tag in required_tags:
                proto_req.required_tags.append(tag)
        if excluded_tags is not None:
            for tag in excluded_tags:
                proto_req.excluded_tags.append(tag)
        proto_req.skip_trimmed = skip_trimmed
        self._set_common_options_proto(proto_req, common_options)
        proto_req.fetch_changed_attributes = fetch_changed_attributes
        enable_structured_response |= fetch_changed_attributes

        response = protobuf_to_dict(
            self._execute_request("object_service", "watch_objects", proto_req)
        )

        for event in response.get("events", []):
            if "history_time" in event:
                event["history_time"] = dict_timestamp_to_microseconds(event["history_time"])
            event["meta"] = event["meta"]["yson"]
            if not request_meta_response:
                event["object_id"] = event["meta"]["key"]  # COMPAT

        if enable_structured_response:
            return response
        else:
            return response.get("events", [])

    def aggregate_objects(
        self,
        object_type,
        group_by=None,
        aggregators=None,
        filter=None,
        timestamp=None,
        enable_structured_response=False,
        common_options=None,
    ):
        if aggregators is None:
            aggregators = []
        if not (isinstance(group_by, list) and len(group_by) > 0):
            raise Exception(
                "Argument group_by must be a non-empty list but got {}".format(group_by)
            )

        proto_req = self.get_object_service_pb2().TReqAggregateObjects()
        proto_req.object_type = self.client_traits.prepare_object_type(object_type)
        self._set_filter_proto(proto_req, filter)
        proto_req.group_by_expressions.expressions.extend(group_by)
        proto_req.aggregate_expressions.expressions.extend(aggregators)
        if timestamp is not None:
            proto_req.timestamp = timestamp
        self._set_common_options_proto(proto_req, common_options)

        proto_rsp = self._execute_request("object_service", "aggregate_objects", proto_req)

        selectors = group_by + aggregators
        results = [
            self._parse_attribute_list_values(result, selectors, enable_structured_response)
            for result in proto_rsp.results
        ]

        if enable_structured_response:
            rsp = dict()
            rsp["results"] = results
            if proto_rsp.HasField("timestamp"):
                rsp["timestamp"] = proto_rsp.timestamp
            return rsp
        else:
            return results

    def _prepare_create_object_request(
        self,
        request,
        object_type,
        attributes,
        update_if_existing=None,
    ):
        request.object_type = self.client_traits.prepare_object_type(object_type)
        if attributes is not None:
            if isinstance(attributes, pb_message.Message):
                request.attributes_payload.protobuf = attributes.SerializeToString()
            else:
                request.attributes_payload.yson = yson.dumps(attributes)
        if update_if_existing is not None:
            proto_if_existing = request.update_if_existing
            proto_if_existing.SetInParent()  # Makes sure the field exists even if empty.
            set_updates = update_if_existing.get("set_updates")
            if set_updates:
                self._fill_set_update_requests(proto_if_existing.set_updates, set_updates)
            set_root_updates = update_if_existing.get("set_root_updates")
            if set_root_updates:
                self._fill_set_root_update_requests(
                    proto_if_existing.set_root_updates,
                    set_root_updates,
                )
            remove_updates = update_if_existing.get("remove_updates")
            if remove_updates:
                self._fill_remove_update_requests(proto_if_existing.remove_updates, remove_updates)
            lock_updates = update_if_existing.get("lock_updates")
            if lock_updates:
                self._fill_lock_update_requests(proto_if_existing.lock_updates, lock_updates)
            method_calls = update_if_existing.get("method_calls")
            if method_calls is not None:
                self._fill_method_calls_requests(proto_if_existing.method_calls, method_calls)
            attribute_timestamp_prerequisites = update_if_existing.get(
                "attribute_timestamp_prerequisites"
            )
            if attribute_timestamp_prerequisites:
                self._fill_attribute_timestamp_prerequisites(
                    proto_if_existing.attribute_timestamp_prerequisites,
                    attribute_timestamp_prerequisites,
                )

    def create_objects(
        self,
        subrequests_parameters,
        transaction_id=None,
        enable_structured_response=False,
        request_meta_response=False,
        common_options=None,
    ):
        proto_req = self.get_object_service_pb2().TReqCreateObjects()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        self._set_common_options_proto(proto_req, common_options)

        for parameters in subrequests_parameters:
            if isinstance(parameters, dict):
                self._prepare_create_object_request(
                    proto_req.subrequests.add(),
                    parameters["object_type"],
                    parameters["attributes"],
                    parameters.get("update_if_existing", None),
                )
            else:
                # Expecting |tuple| or |list| type of |parameters|.
                object_type, attributes = parameters
                self._prepare_create_object_request(
                    proto_req.subrequests.add(), object_type, attributes
                )

        response = protobuf_to_dict(
            self._execute_request("object_service", "create_objects", proto_req)
        )

        for subresponse in response.get("subresponses", []):
            subresponse["meta"] = subresponse["meta"]["yson"]

        if enable_structured_response:
            if not request_meta_response:
                for subresponse in response.get("subresponses", []):
                    subresponse["object_id"] = subresponse["meta"]["key"]  # COMPAT
            return response
        else:
            return [subresponse["meta"]["key"] for subresponse in response.get("subresponses", [])]

    def create_object(
        self,
        object_type,
        attributes=None,
        transaction_id=None,
        enable_structured_response=False,
        request_meta_response=False,
        update_if_existing=None,
        common_options=None,
    ):
        proto_req = self.get_object_service_pb2().TReqCreateObject()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        self._set_common_options_proto(proto_req, common_options)

        self._prepare_create_object_request(
            proto_req,
            object_type,
            attributes,
            update_if_existing,
        )

        response = protobuf_to_dict(
            self._execute_request("object_service", "create_object", proto_req)
        )

        response["meta"] = response["meta"]["yson"]
        if enable_structured_response:
            if not request_meta_response:
                response["object_id"] = response["meta"]["key"]  # COMPAT
            return response
        else:
            return response["meta"]["key"]

    def _prepare_remove_object_request(self, req, object_type, object_identity):
        req.object_type = self.client_traits.prepare_object_type(object_type)
        self._fill_object_identity(req, object_identity)

    def remove_objects(
        self,
        iterable_object_type_ids,
        transaction_id=None,
        ignore_nonexistent=None,
        common_options=None,
        allow_removal_with_non_empty_reference=None,
    ):
        proto_req = self.get_object_service_pb2().TReqRemoveObjects()
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        if ignore_nonexistent is not None:
            proto_req.ignore_nonexistent = ignore_nonexistent
        if allow_removal_with_non_empty_reference is not None:
            proto_req.allow_removal_with_non_empty_reference = (
                allow_removal_with_non_empty_reference
            )

        for object_type, object_identity in iterable_object_type_ids:
            self._prepare_remove_object_request(
                proto_req.subrequests.add(),
                object_type,
                object_identity,
            )
        self._set_common_options_proto(proto_req, common_options)

        return protobuf_to_dict(
            self._execute_request("object_service", "remove_objects", proto_req)
        )

    def remove_object(
        self,
        object_type,
        object_identity=_SENTINEL,
        transaction_id=None,
        ignore_nonexistent=None,
        common_options=None,
        allow_removal_with_non_empty_reference=None,
        object_id=None,  # COMPAT
    ):
        # COMPAT this makes object_id an alias for object_identity. This will provide a migration
        # path for callers that use named arguments.
        if object_id is not None:
            assert object_identity is _SENTINEL
            object_identity = object_id
        assert object_identity is not _SENTINEL

        proto_req = self.get_object_service_pb2().TReqRemoveObject()
        self._prepare_remove_object_request(proto_req, object_type, object_identity)
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        if ignore_nonexistent is not None:
            proto_req.ignore_nonexistent = ignore_nonexistent
        if allow_removal_with_non_empty_reference is not None:
            proto_req.allow_removal_with_non_empty_reference = (
                allow_removal_with_non_empty_reference
            )
        self._set_common_options_proto(proto_req, common_options)

        return protobuf_to_dict(self._execute_request("object_service", "remove_object", proto_req))

    def _prepare_update_object_request(
        self,
        req,
        object_type,
        object_identity,
        set_updates,
        set_root_updates,
        remove_updates,
        attribute_timestamp_prerequisites,
        lock_updates,
        method_calls,
    ):
        req.object_type = self.client_traits.prepare_object_type(object_type)
        self._fill_object_identity(req, object_identity)
        if set_updates is not None:
            self._fill_set_update_requests(req.set_updates, set_updates)
        if set_root_updates is not None:
            self._fill_set_root_update_requests(req.set_root_updates, set_root_updates)
        if remove_updates is not None:
            self._fill_remove_update_requests(req.remove_updates, remove_updates)
        if lock_updates is not None:
            self._fill_lock_update_requests(req.lock_updates, lock_updates)
        if method_calls is not None:
            self._fill_method_calls_requests(req.method_calls, method_calls)
        if attribute_timestamp_prerequisites is not None:
            self._fill_attribute_timestamp_prerequisites(
                req.attribute_timestamp_prerequisites, attribute_timestamp_prerequisites
            )

    def update_objects(
        self,
        subrequests_parameters,
        transaction_id=None,
        common_options=None,
        ignore_nonexistent=None,
    ):
        proto_req = self.get_object_service_pb2().TReqUpdateObjects()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        if ignore_nonexistent is not None:
            proto_req.ignore_nonexistent = ignore_nonexistent
        for parameters in subrequests_parameters:
            self._prepare_update_object_request(
                proto_req.subrequests.add(),
                parameters["object_type"],
                self._get_object_identity(parameters),
                parameters.get("set_updates", None),
                parameters.get("set_root_updates", None),
                parameters.get("remove_updates", None),
                parameters.get("attribute_timestamp_prerequisites", None),
                parameters.get("lock_updates", None),
                parameters.get("method_calls", None),
            )
        self._set_common_options_proto(proto_req, common_options)

        return protobuf_to_dict(
            self._execute_request("object_service", "update_objects", proto_req)
        )

    def update_object(
        self,
        object_type,
        object_identity=_SENTINEL,
        set_updates=None,
        remove_updates=None,
        attribute_timestamp_prerequisites=None,
        transaction_id=None,
        lock_updates=None,
        common_options=None,
        set_root_updates=None,
        method_calls=None,
        ignore_nonexistent=None,
        object_id=None,  # COMPAT
    ):
        # COMPAT this makes object_id an alias for object_identity. This will provide a migration
        # path for callers that use named arguments.
        if object_id is not None:
            assert object_identity is _SENTINEL
            object_identity = object_id
        assert object_identity is not _SENTINEL

        proto_req = self.get_object_service_pb2().TReqUpdateObject()
        proto_req.format = object_pb2.EPayloadFormat.Value("PF_YSON")
        if transaction_id is not None:
            proto_req.transaction_id = transaction_id
        if ignore_nonexistent is not None:
            proto_req.ignore_nonexistent = ignore_nonexistent
        self._prepare_update_object_request(
            proto_req,
            object_type,
            object_identity,
            set_updates,
            set_root_updates,
            remove_updates,
            attribute_timestamp_prerequisites,
            lock_updates,
            method_calls,
        )
        self._set_common_options_proto(proto_req, common_options)

        return protobuf_to_dict(self._execute_request("object_service", "update_object", proto_req))

    def check_object_permissions(
        self,
        subrequests,
        timestamp=None,
        common_options=None,
        enable_structured_response=False,
    ):
        proto_req = self.get_object_service_pb2().TReqCheckObjectPermissions()
        if timestamp is not None:
            proto_req.timestamp = timestamp
        for subrequest in subrequests:
            proto_subreq = proto_req.subrequests.add()
            self._fill_object_identity(proto_subreq, self._get_object_identity(subrequest))
            proto_subreq.object_type = self.client_traits.prepare_object_type(subrequest["object_type"])
            proto_subreq.subject_id = subrequest["subject_id"]
            proto_subreq.permission = self.client_traits.prepare_access_control_permission(
                subrequest["permission"]
            )
            if subrequest.get("attribute_path") is not None:
                proto_subreq.attribute_path = subrequest["attribute_path"]
        self._set_common_options_proto(proto_req, common_options)

        response = protobuf_to_dict(
            self._execute_request("object_service", "check_object_permissions", proto_req)
        )

        if enable_structured_response:
            return response
        else:
            results = []
            for subresponse in response["subresponses"]:
                result = {"action": subresponse["action"]}
                if "object_id" in subresponse and subresponse["object_id"] != "":
                    result["object_id"] = subresponse["object_id"]
                if "object_type" in subresponse and subresponse["object_type"] != "null":
                    result["object_type"] = subresponse["object_type"]
                if "subject_id" in subresponse and subresponse["subject_id"] != "":
                    result["subject_id"] = subresponse["subject_id"]
                results.append(result)
            return results

    def get_object_access_allowed_for(self, subrequests, timestamp=None):
        proto_req = self.get_object_service_pb2().TReqGetObjectAccessAllowedFor()
        if timestamp is not None:
            proto_req.timestamp = timestamp
        for subrequest in subrequests:
            proto_subreq = proto_req.subrequests.add()
            self._fill_object_identity(proto_subreq, self._get_object_identity(subrequest))
            proto_subreq.object_type = self.client_traits.prepare_object_type(subrequest["object_type"])
            proto_subreq.permission = self.client_traits.prepare_access_control_permission(
                subrequest["permission"]
            )
            if subrequest.get("attribute_path") is not None:
                proto_subreq.attribute_path = subrequest["attribute_path"]

        response = protobuf_to_dict(
            self._execute_request("object_service", "get_object_access_allowed_for", proto_req)
        )

        results = []
        for subresponse in response["subresponses"]:
            result = {"user_ids": subresponse["user_ids"]}
            results.append(result)
        return results

    def get_user_access_allowed_to(self, subrequests):
        proto_req = self.get_object_service_pb2().TReqGetUserAccessAllowedTo()
        for subrequest in subrequests:
            proto_subreq = proto_req.subrequests.add()
            proto_subreq.user_id = subrequest["user_id"]
            proto_subreq.object_type = self.client_traits.prepare_object_type(subrequest["object_type"])
            proto_subreq.permission = self.client_traits.prepare_access_control_permission(
                subrequest["permission"]
            )
            if subrequest.get("attribute_path") is not None:
                proto_subreq.attribute_path = subrequest["attribute_path"]
            self._set_filter_proto(proto_subreq, subrequest.get("filter"))
            continuation_token = subrequest.get("continuation_token")
            if continuation_token is not None:
                proto_subreq.continuation_token = continuation_token
            limit = subrequest.get("limit")
            if limit is not None:
                proto_subreq.limit = limit

        response = protobuf_to_dict(
            self._execute_request("object_service", "get_user_access_allowed_to", proto_req)
        )

        results = []
        for subresponse in response.get("subresponses", []):
            results.append(
                dict(
                    object_ids=subresponse.get("object_ids", []),
                    continuation_token=subresponse.get("continuation_token"),
                )
            )
        return results

    def generate_timestamp(self, enable_structured_response=False):
        proto_req = self.get_object_service_pb2().TReqGenerateTimestamp()

        response = protobuf_to_dict(
            self._execute_request("object_service", "generate_timestamp", proto_req)
        )

        if enable_structured_response:
            return response
        else:
            return response["timestamp"]

    def start_transaction(
        self,
        enable_structured_response=False,
        start_timestamp=None,
        underlying_transaction_id=None,
        underlying_transaction_address=None,
        common_options=None,
        transaction_context=None,
        skip_watch_log=None,
        skip_history=None,
        skip_revision_bump=None,
    ):
        proto_req = self.get_object_service_pb2().TReqStartTransaction()
        if start_timestamp is not None:
            proto_req.start_timestamp = start_timestamp
        if underlying_transaction_id is not None:
            proto_req.underlying_transaction_id = underlying_transaction_id
        if underlying_transaction_address is not None:
            proto_req.underlying_transaction_address = underlying_transaction_address
        if skip_watch_log is not None:
            proto_req.mutating_transaction_options.skip_watch_log = skip_watch_log
        if skip_history is not None:
            proto_req.mutating_transaction_options.skip_history = skip_history
        if skip_revision_bump is not None:
            proto_req.mutating_transaction_options.skip_revision_bump = skip_revision_bump
        if transaction_context:
            for k, v in transaction_context.items():
                proto_req.mutating_transaction_options.transaction_context.items[k] = v
        self._set_common_options_proto(proto_req, common_options)

        rsp = self._execute_request("object_service", "start_transaction", proto_req)

        if enable_structured_response:
            response = {
                "transaction_id": rsp.transaction_id,
            }
            if rsp.HasField("start_timestamp"):
                response["start_timestamp"] = rsp.start_timestamp
            if rsp.HasField("start_time"):
                response["start_time"] = proto_timestamp_to_microseconds(rsp.start_time)
            return response

        else:
            return rsp.transaction_id

    def commit_transaction(self, transaction_id, transaction_context=None):
        proto_req = self.get_object_service_pb2().TReqCommitTransaction()
        proto_req.transaction_id = transaction_id
        if transaction_context:
            for k, v in transaction_context.items():
                proto_req.transaction_context.items[k] = v

        rsp = self._execute_request("object_service", "commit_transaction", proto_req)

        response = {"commit_timestamp": rsp.commit_timestamp}
        if rsp.HasField("start_time"):
            response["start_time"] = proto_timestamp_to_microseconds(rsp.start_time)
        if rsp.HasField("finish_time"):
            response["finish_time"] = proto_timestamp_to_microseconds(rsp.finish_time)
        if rsp.HasField("performance_statistics"):
            response["performance_statistics"] = protobuf_to_dict(rsp.performance_statistics)
        return response

    def abort_transaction(self, transaction_id):
        proto_req = self.get_object_service_pb2().TReqAbortTransaction()
        proto_req.transaction_id = transaction_id

        return protobuf_to_dict(
            self._execute_request("object_service", "abort_transaction", proto_req)
        )

    def get_masters(self, _allow_retries=True):
        proto_req = self.get_discovery_service_pb2().TReqGetMasters()

        return protobuf_to_dict(
            self._execute_request(
                "discovery_service",
                "get_masters",
                proto_req,
                allow_discovery=False,
                allow_retries=_allow_retries,
            )
        )
