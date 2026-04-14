"""Kazoo Exceptions"""

from collections import defaultdict


class KazooException(Exception):
    """Base Kazoo exception that all other kazoo library exceptions
    inherit from"""


class ZookeeperError(KazooException):
    """Base Zookeeper exception for errors originating from the
    Zookeeper server"""


class CancelledError(KazooException):
    """Raised when a process is cancelled by another thread"""


class ConfigurationError(KazooException):
    """Raised if the configuration arguments to an object are
    invalid"""


class ZookeeperStoppedError(KazooException):
    """Raised when the kazoo client stopped (and thus not connected)"""


class ConnectionDropped(KazooException):
    """Internal error for jumping out of loops"""


class LockTimeout(KazooException):
    """Raised if failed to acquire a lock.

    .. versionadded:: 1.1
    """


class WriterNotClosedException(KazooException):
    """Raised if the writer is unable to stop closing when requested.

    .. versionadded:: 1.2
    """


class SASLException(KazooException):
    """Raised if SASL encountered a (local) error.

    .. versionadded:: 2.7.0
    """


def _invalid_error_code():
    raise RuntimeError("Invalid error code")


EXCEPTIONS = defaultdict(_invalid_error_code)


def _zookeeper_exception(code):
    def decorator(klass):
        EXCEPTIONS[code] = klass
        klass.code = code
        return klass

    return decorator


# Pulled from zookeeper-server/src/main/java/org/apache/zookeeper/
# KeeperException.java in the Java Zookeeper server source code.


@_zookeeper_exception(0)
class RolledBackError(ZookeeperError):
    pass


@_zookeeper_exception(-1)
class SystemZookeeperError(ZookeeperError):
    """System and server-side errors.
    This is never thrown by the server, it shouldn't be used other than to
    indicate a range. Specifically error codes greater than this value, but
    lesser than APIError, are system errors.
    """


@_zookeeper_exception(-2)
class RuntimeInconsistency(ZookeeperError):
    """A runtime inconsistency was found."""


@_zookeeper_exception(-3)
class DataInconsistency(ZookeeperError):
    """A data inconsistency was found."""


@_zookeeper_exception(-4)
class ConnectionLoss(ZookeeperError):
    """Connection to the server has been lost."""


@_zookeeper_exception(-5)
class MarshallingError(ZookeeperError):
    """Error while marshalling or unmarshalling data."""


@_zookeeper_exception(-6)
class UnimplementedError(ZookeeperError):
    """Operation is unimplemented."""


@_zookeeper_exception(-7)
class OperationTimeoutError(ZookeeperError):
    """Operation timeout."""


@_zookeeper_exception(-8)
class BadArgumentsError(ZookeeperError):
    """Invalid arguments."""


@_zookeeper_exception(-12)
class UnknownSessionError(ZookeeperError):
    """Unknown session (internal server use only)."""


@_zookeeper_exception(-13)
class NewConfigNoQuorumError(ZookeeperError):
    """No quorum of new config is connected and up-to-date with the leader of
    last committed config - try invoking reconfiguration after new servers are
    connected and synced.
    """


@_zookeeper_exception(-14)
class ReconfigInProcessError(ZookeeperError):
    """Another reconfiguration is in progress -- concurrent reconfigs not
    supported (yet).
    """


@_zookeeper_exception(-100)
class APIError(ZookeeperError):
    """API errors.
    This is never thrown by the server, it shouldn't be used other than to
    indicate a range. Specifically error codes greater than this value are API
    errors (while values less than this indicate a system error.
    """


@_zookeeper_exception(-101)
class NoNodeError(ZookeeperError):
    """Node does not exist."""


@_zookeeper_exception(-102)
class NoAuthError(ZookeeperError):
    """Not authenticated."""


@_zookeeper_exception(-103)
class BadVersionError(ZookeeperError):
    """Version conflict. In case of reconfiguration: reconfig requested from
    config version X but last seen config has a different version Y.
    """


@_zookeeper_exception(-108)
class NoChildrenForEphemeralsError(ZookeeperError):
    """Ephemeral nodes may not have children."""


@_zookeeper_exception(-110)
class NodeExistsError(ZookeeperError):
    """The node already exists."""


@_zookeeper_exception(-111)
class NotEmptyError(ZookeeperError):
    """The node has children."""


@_zookeeper_exception(-112)
class SessionExpiredError(ZookeeperError):
    """The session has been expired by the server."""


@_zookeeper_exception(-113)
class InvalidCallbackError(ZookeeperError):
    """Invalid callback specified."""


@_zookeeper_exception(-114)
class InvalidACLError(ZookeeperError):
    """Invalid ACL specified"""


@_zookeeper_exception(-115)
class AuthFailedError(ZookeeperError):
    """Client authentication failed."""


@_zookeeper_exception(-118)
class SessionMovedError(ZookeeperError):
    """Session moved to another server, so operation is ignored."""


@_zookeeper_exception(-119)
class NotReadOnlyCallError(ZookeeperError):
    """An API call that is not read-only was used while connected to a
    read-only server.
    """


@_zookeeper_exception(-120)
class EphemeralOnLocalSessionError(ZookeeperError):
    """Attempt to create ephemeral node on a local session."""


@_zookeeper_exception(-121)
class NoWatcherError(ZookeeperError):
    """Attempts to remove a non-existing watcher."""


@_zookeeper_exception(-122)
class RequestTimeoutError(ZookeeperError):
    """Request not completed within max allowed time."""


@_zookeeper_exception(-123)
class ReconfigDisabledError(ZookeeperError):
    """Attempts to perform a reconfiguration operation when reconfiguration
    feature is disabled.
    """


@_zookeeper_exception(-124)
class SessionClosedRequireSaslError(ZookeeperError):
    """The session has been closed by server because server requires client to
    do authentication with configured authentication scheme at the server, but
    client is not configured with required authentication scheme or configured
    but authentication failed (i.e. wrong credential used.).
    """


@_zookeeper_exception(-125)
class QuotaExceededError(ZookeeperError):
    """Exceeded the quota that was set on the path."""


@_zookeeper_exception(-127)
class ThrottledOpError(ZookeeperError):
    """Operation was throttled and not executed at all. This error code
    indicates that zookeeper server is under heavy load and can't process
    incoming requests at full speed; please retry with back off.
    """


class ConnectionClosedError(SessionExpiredError):
    """Connection is closed"""


# BW Compat aliases for C lib style exceptions
ConnectionLossException = ConnectionLoss
MarshallingErrorException = MarshallingError
SystemErrorException = SystemZookeeperError
RuntimeInconsistencyException = RuntimeInconsistency
DataInconsistencyException = DataInconsistency
UnimplementedException = UnimplementedError
OperationTimeoutException = OperationTimeoutError
BadArgumentsException = BadArgumentsError
ApiErrorException = APIError
NoNodeException = NoNodeError
NoAuthException = NoAuthError
BadVersionException = BadVersionError
NoChildrenForEphemeralsException = NoChildrenForEphemeralsError
NodeExistsException = NodeExistsError
InvalidACLException = InvalidACLError
AuthFailedException = AuthFailedError
NotEmptyException = NotEmptyError
SessionExpiredException = SessionExpiredError
InvalidCallbackException = InvalidCallbackError
