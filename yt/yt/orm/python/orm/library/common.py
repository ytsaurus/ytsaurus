from yt_proto.yt.orm.client.proto import error_pb2

import yt_proto.yt.core.yson.proto.protobuf_interop_pb2 as yson_protobuf_interop_pb2

# TODO(bidzilya): Assuming future rework of factory-like class yt.wrapper.errors.YtResponseError
#                 to actual factory method yt.wrapper.errors.make_yt_response_error.
try:
    from yt.wrapper.errors import create_response_error as make_yt_response_error_base
except ImportError:
    from yt.wrapper.errors import YtResponseError as make_yt_response_error_base
from yt.wrapper.dynamic_table_commands import (
    get_dynamic_table_retriable_errors as get_yt_retriable_errors,
)

from yt.wrapper.errors import YtTabletTransactionLockConflict

from yt.wrapper.retries import run_with_retries

from yt_yson_bindings import loads_proto, dumps_proto
import yt.yson as yson

from yt.common import YtError, YtResponseError

try:
    from yt.packages.six import itervalues, PY3
    from yt.packages.six.moves import xrange
except ImportError:
    from six import itervalues, PY3
    from six.moves import xrange

import grpc

import google.protobuf.timestamp_pb2 as timestamp_pb2

import string
import time


class ClientError(YtError):
    pass


# Response contains invalid structure.
class InvalidDiscoveryResponse(ClientError):
    pass


# Search for alive instance with required properties (e.g.: transport support) failed.
class InstanceDiscoveryError(ClientError):
    pass


################################################################################


class IncorrectResponseError(YtError):
    pass


class NotEnoughResourcesError(YtResponseError):
    pass


class InvalidObjectIdError(YtResponseError):
    pass


class DuplicateObjectIdError(YtResponseError):
    pass


class NoSuchObjectError(YtResponseError):
    pass


class InvalidObjectTypeError(YtResponseError):
    pass


class AuthenticationError(YtResponseError):
    pass


class AuthorizationError(YtResponseError):
    pass


class InvalidTransactionStateError(YtResponseError):
    pass


class InvalidTransactionIdError(YtResponseError):
    pass


class InvalidObjectStateError(YtResponseError):
    pass


class NoSuchTransactionError(YtResponseError):
    pass


class UserBannedError(YtResponseError):
    pass


class PrerequisiteCheckFailureError(YtResponseError):
    pass


class InvalidContinuationTokenError(YtResponseError):
    pass


class RowsAlreadyTrimmedError(YtResponseError):
    pass


class InvalidObjectSpecError(YtResponseError):
    pass


class ContinuationTokenVersionMismatchError(YtResponseError):
    pass


class TimestampOutOfRangeError(YtResponseError):
    pass


class RequestThrottledError(YtResponseError):
    pass


class UnstableDatabaseSettingsError(YtResponseError):
    pass


class EntryNotFoundError(YtResponseError):
    pass


class DuplicateRequestError(YtResponseError):
    pass


class NoSuchIndex(YtResponseError):
    pass


class NotWatchableError(YtResponseError):
    pass


class TooManyAffectedObjects(YtResponseError):
    pass


class IndexNotApplicable(YtResponseError):
    pass


class WatchesConfigurationMismatch(YtResponseError):
    pass


class InvalidRequestArgumentsError(YtResponseError):
    pass


class UniqueValueAlreadyExists(YtResponseError):
    pass


class LimitTooLargeError(YtResponseError):
    pass


class SemaphoreFullError(YtResponseError):
    pass


class RemovalForbiddenError(YtResponseError):
    pass


# TODO(bidzilya): Use canonical functions from yt.common.
def validate_error_recursively(error, validator):
    if validator(error):
        return True
    for inner_error in error.get("inner_errors", []):
        if validate_error_recursively(inner_error, validator):
            return True
    return False


def contains_code(error, error_code):
    return validate_error_recursively(
        error,
        lambda inner_error: int(inner_error.get("code", 0)) == error_code,
    )


def get_yt_response_error_code_to_class():
    return {
        error_pb2.EC_INVALID_OBJECT_ID: InvalidObjectIdError,
        error_pb2.EC_DUPLICATE_OBJECT_ID: DuplicateObjectIdError,
        error_pb2.EC_NO_SUCH_OBJECT: NoSuchObjectError,
        error_pb2.EC_INVALID_OBJECT_TYPE: InvalidObjectTypeError,
        error_pb2.EC_AUTHENTICATION_ERROR: AuthenticationError,
        error_pb2.EC_AUTHORIZATION_ERROR: AuthorizationError,
        error_pb2.EC_INVALID_TRANSACTION_STATE: InvalidTransactionStateError,
        error_pb2.EC_INVALID_TRANSACTION_ID: InvalidTransactionIdError,
        error_pb2.EC_INVALID_OBJECT_STATE: InvalidObjectStateError,
        error_pb2.EC_NO_SUCH_TRANSACTION: NoSuchTransactionError,
        error_pb2.EC_USER_BANNED: UserBannedError,
        error_pb2.EC_PREREQUISITE_CHECK_FAILURE: PrerequisiteCheckFailureError,
        error_pb2.EC_INVALID_CONTINUATION_TOKEN: InvalidContinuationTokenError,
        error_pb2.EC_ROWS_ALREADY_TRIMMED: RowsAlreadyTrimmedError,
        error_pb2.EC_INVALID_OBJECT_SPEC: InvalidObjectSpecError,
        error_pb2.EC_CONTINUATION_TOKEN_VERSION_MISMATCH: ContinuationTokenVersionMismatchError,
        error_pb2.EC_TIMESTAMP_OUT_OF_RANGE: TimestampOutOfRangeError,
        error_pb2.EC_REQUEST_THROTTLED: RequestThrottledError,
        error_pb2.EC_UNSTABLE_DATABASE_SETTINGS: UnstableDatabaseSettingsError,
        error_pb2.EC_ENTRY_NOT_FOUND: EntryNotFoundError,
        error_pb2.EC_DUPLICATE_REQUEST: DuplicateRequestError,
        error_pb2.EC_NOT_WATCHABLE: NotWatchableError,
        error_pb2.EC_TOO_MANY_AFFECTED_OBJECTS: TooManyAffectedObjects,
        error_pb2.EC_INDEX_NOT_APPLICABLE: IndexNotApplicable,
        error_pb2.EC_NO_SUCH_INDEX: NoSuchIndex,
        error_pb2.EC_WATCHES_CONFIGURATION_MISMATCH: WatchesConfigurationMismatch,
        error_pb2.EC_INVALID_REQUEST_ARGUMENTS: InvalidRequestArgumentsError,
        error_pb2.EC_UNIQUE_VALUE_ALREADY_EXISTS: UniqueValueAlreadyExists,
        error_pb2.EC_LIMIT_TOO_LARGE: LimitTooLargeError,
        error_pb2.EC_SEMAPHORE_FULL: SemaphoreFullError,
        error_pb2.EC_REMOVAL_FORBIDDEN: RemovalForbiddenError,
    }


def make_yt_response_error(base_error, method, request_id, code_to_class=None):
    if code_to_class is None:
        code_to_class = get_yt_response_error_code_to_class()

    error_factory = make_yt_response_error_base
    for code in code_to_class.keys():
        if contains_code(base_error, code):
            error_factory = code_to_class[code]
            break

    error = error_factory(base_error)
    error.message = "Method /{0} replied with error".format(method)
    error.attributes.update(method=method, request_id=request_id)

    return error


# We need exceptions for the retries purpose
# but GRPC provides only error codes in the public interface,
# so we define our own exceptions hierarchy.
class GrpcError(YtError):
    pass


class GrpcDeadlineExceededError(GrpcError):
    pass


class GrpcUnavailableError(GrpcError):
    pass


class GrpcResourceExhaustedError(GrpcError):
    pass


def make_grpc_error(base_error):
    code_to_class = {
        grpc.StatusCode.DEADLINE_EXCEEDED: GrpcDeadlineExceededError,
        grpc.StatusCode.UNAVAILABLE: GrpcUnavailableError,
        grpc.StatusCode.RESOURCE_EXHAUSTED: GrpcResourceExhaustedError,
    }
    base_error_code = base_error.code() if hasattr(base_error, "code") else None
    error_factory = code_to_class.get(base_error_code, GrpcError)
    return error_factory(
        "GRPC transport layer error occurred",
        attributes=dict(
            debug_error_string=base_error.debug_error_string(),
            status=base_error.code(),
            details=base_error.details(),
        ),
    )


def get_retriable_errors():
    # HTTP errors (due to python requests library) are included into the get_yt_retriable_errors.
    result = get_yt_retriable_errors()
    result += (IncorrectResponseError, InstanceDiscoveryError)
    result += (GrpcDeadlineExceededError, GrpcUnavailableError, grpc.FutureTimeoutError)
    return result


def camel_case_to_underscore_case(str):
    result = []
    first = True
    for c in str:
        if c in string.ascii_uppercase:
            if not first:
                result.append("_")
            c = c.lower()
        result.append(c)
        first = False
    return "".join(result)


def underscore_case_to_camel_case(str):
    result = []
    first = True
    upper = True
    for c in str:
        if c == "_":
            upper = True
        else:
            if upper:
                if c not in string.ascii_letters and not first:
                    result.append("_")
                c = c.upper()
            result.append(c)
            upper = False
        first = False
    return "".join(result)


def get_master_instance_tag(uuid):
    parts = list(reversed([int(part, 16) for part in uuid.split("-")]))
    return (parts[1] >> 24) & 0x00FF


def strip_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix) :]
    else:
        return string


class LazyLogField(object):
    def __init__(self, func):
        self._func = func
        self._value = None

    def __str__(self):
        if self._value is None:
            self._value = self._func()
        return self._value


def milliseconds_to_seconds(milliseconds):
    if milliseconds is None:
        return None
    return milliseconds / 1000.0


def seconds_to_milliseconds(seconds):
    if seconds is None:
        return None
    return seconds * 1000.0


def proto_timestamp_to_microseconds(proto_timestamp):
    return proto_timestamp.seconds * (10**6) + proto_timestamp.nanos // 1000


def dict_timestamp_to_microseconds(dict_timestamp):
    return dict_timestamp.get("seconds", 0) * (10**6) + dict_timestamp.get("nanos", 0) // 1000


def microseconds_to_proto_timestamp(microseconds):
    proto_timestamp = timestamp_pb2.Timestamp()
    proto_timestamp.seconds = microseconds // (10**6)
    proto_timestamp.nanos = (microseconds % (10**6)) * 1000
    return proto_timestamp


class WaitFailed(Exception):
    pass


def wait(predicate, error_message=None, iter=100, sleep_backoff=1, ignore_exceptions=False):
    for i in xrange(iter):
        try:
            if predicate():
                return
        except Exception:
            if ignore_exceptions and i + 1 < iter:
                time.sleep(sleep_backoff)
                continue
            raise
        time.sleep(sleep_backoff)

    if error_message is None:
        error_message = "Wait failed"
    error_message += " (timeout = {0})".format(iter * sleep_backoff)
    raise WaitFailed(error_message)


def iter_proto_enum_value_names(proto_enum, exclude=None):
    for value in proto_enum.DESCRIPTOR.values:
        extension = yson_protobuf_interop_pb2.enum_value_name
        v = str(value.GetOptions().Extensions[extension])
        if v != exclude:
            yield v


def iter_proto_message_types(module):
    def traverse(type):
        yield type
        for nested_type in type.nested_types:
            for result_type in traverse(nested_type):
                yield result_type

    for type in itervalues(module.DESCRIPTOR.message_types_by_name):
        for result_type in traverse(type):
            yield result_type


def iter_proto_enum_types(module):
    for enum_type in itervalues(module.DESCRIPTOR.enum_types_by_name):
        yield enum_type

    for message_type in iter_proto_message_types(module):
        for enum_type in message_type.enum_types:
            yield enum_type


def protobuf_to_dict(message):
    return yson.loads(dumps_proto(message))


def dict_to_protobuf(obj, proto_class, skip_unknown_fields=None):
    return loads_proto(yson.dumps(obj), proto_class, skip_unknown_fields=skip_unknown_fields)


def try_read_from_file(file_name, on_error):
    exceptions = [IOError, ValueError]
    if PY3:
        exceptions.append(FileNotFoundError)  # noqa
    output = None
    try:
        with open(file_name) as fin:
            output = fin.read().strip()
    except tuple(exceptions):
        on_error()
    return output


def with_lock_conflict_retries(action, **retry_kwargs):
    def wrapper(*args, **kwargs):
        return run_with_retries(
            lambda: action(*args, **kwargs),
            exceptions=(YtTabletTransactionLockConflict,),
            **retry_kwargs
        )

    return wrapper
