# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Type stubs for confluent_kafka.cimpl

This combines automatic stubgen output (constants, functions) with
manual class definitions based on runtime introspection and domain knowledge.

⚠️ WARNING: MAINTENANCE REQUIRED ⚠️
This stub file must be kept in sync with the C extension source code in src/:
- src/Admin.c (AdminClientImpl methods)
- src/Producer.c (Producer class)
- src/Consumer.c (Consumer class)
- src/AdminTypes.c (NewTopic, NewPartitions classes)
- src/confluent_kafka.c (KafkaError, Message, TopicPartition, Uuid classes)

When modifying C extension interfaces (method signatures, parameters, defaults),
you MUST update the corresponding type definitions in this file.
Failure to do so will result in incorrect type hints and mypy errors.

TODO: Consider migrating to Cython in the future to eliminate this dual
maintenance burden and get type hints directly from the implementation.
"""

import builtins
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union, overload

try:
    from typing import Self
except ImportError:
    # FIXME: drop fallback once we require Python >= 3.11
    from typing_extensions import Self

from confluent_kafka.admin._metadata import ClusterMetadata, GroupMetadata

from ._model import AcknowledgeType, Messages
from ._types import HeadersType

# Callback types with proper class references (defined locally to avoid circular imports)
DeliveryCallback = Callable[[Optional['KafkaError'], 'Message'], None]
RebalanceCallback = Callable[['Consumer', List['TopicPartition']], None]
# (offsets, exception) — note the order is offsets-first, opposite of on_commit.
AcknowledgementCommitCallback = Callable[
    [Dict['TopicPartition', Set[int]], Optional['KafkaException']], None
]

# ===== CLASSES (Manual - stubgen missed these) =====

class KafkaError:
    BROKER_NOT_AVAILABLE: int
    CLUSTER_AUTHORIZATION_FAILED: int
    CONCURRENT_TRANSACTIONS: int
    COORDINATOR_LOAD_IN_PROGRESS: int
    COORDINATOR_NOT_AVAILABLE: int
    DELEGATION_TOKEN_AUTHORIZATION_FAILED: int
    DELEGATION_TOKEN_AUTH_DISABLED: int
    DELEGATION_TOKEN_EXPIRED: int
    DELEGATION_TOKEN_NOT_FOUND: int
    DELEGATION_TOKEN_OWNER_MISMATCH: int
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: int
    DUPLICATE_RESOURCE: int
    DUPLICATE_SEQUENCE_NUMBER: int
    DUPLICATE_VOTER: int
    ELECTION_NOT_NEEDED: int
    ELIGIBLE_LEADERS_NOT_AVAILABLE: int
    FEATURE_UPDATE_FAILED: int
    FENCED_INSTANCE_ID: int
    FENCED_LEADER_EPOCH: int
    FENCED_MEMBER_EPOCH: int
    FENCED_STATE_EPOCH: int
    FETCH_SESSION_ID_NOT_FOUND: int
    GROUP_AUTHORIZATION_FAILED: int
    GROUP_ID_NOT_FOUND: int
    GROUP_MAX_SIZE_REACHED: int
    GROUP_SUBSCRIBED_TO_TOPIC: int
    ILLEGAL_GENERATION: int
    ILLEGAL_SASL_STATE: int
    INCONSISTENT_GROUP_PROTOCOL: int
    INCONSISTENT_TOPIC_ID: int
    INCONSISTENT_VOTER_SET: int
    INVALID_COMMIT_OFFSET_SIZE: int
    INVALID_CONFIG: int
    INVALID_FETCH_SESSION_EPOCH: int
    INVALID_GROUP_ID: int
    INVALID_MSG: int
    INVALID_MSG_SIZE: int
    INVALID_PARTITIONS: int
    INVALID_PRINCIPAL_TYPE: int
    INVALID_PRODUCER_EPOCH: int
    INVALID_PRODUCER_ID_MAPPING: int
    INVALID_RECORD: int
    INVALID_RECORD_STATE: int
    INVALID_REGISTRATION: int
    INVALID_REGULAR_EXPRESSION: int
    INVALID_REPLICATION_FACTOR: int
    INVALID_REPLICA_ASSIGNMENT: int
    INVALID_REQUEST: int
    INVALID_REQUIRED_ACKS: int
    INVALID_SHARE_SESSION_EPOCH: int
    INVALID_SESSION_TIMEOUT: int
    INVALID_TIMESTAMP: int
    INVALID_TRANSACTION_TIMEOUT: int
    INVALID_TXN_STATE: int
    INVALID_UPDATE_VERSION: int
    INVALID_VOTER_KEY: int
    KAFKA_STORAGE_ERROR: int
    LEADER_NOT_AVAILABLE: int
    LISTENER_NOT_FOUND: int
    LOG_DIR_NOT_FOUND: int
    MEMBER_ID_REQUIRED: int
    MISMATCHED_ENDPOINT_TYPE: int
    MSG_SIZE_TOO_LARGE: int
    NETWORK_EXCEPTION: int
    NON_EMPTY_GROUP: int
    NOT_CONTROLLER: int
    NOT_COORDINATOR: int
    NOT_ENOUGH_REPLICAS: int
    NOT_ENOUGH_REPLICAS_AFTER_APPEND: int
    NOT_LEADER_FOR_PARTITION: int
    NO_ERROR: int
    NO_REASSIGNMENT_IN_PROGRESS: int
    OFFSET_METADATA_TOO_LARGE: int
    OFFSET_NOT_AVAILABLE: int
    OFFSET_OUT_OF_RANGE: int
    OPERATION_NOT_ATTEMPTED: int
    OUT_OF_ORDER_SEQUENCE_NUMBER: int
    POLICY_VIOLATION: int
    PREFERRED_LEADER_NOT_AVAILABLE: int
    PRINCIPAL_DESERIALIZATION_FAILURE: int
    PRODUCER_FENCED: int
    REASSIGNMENT_IN_PROGRESS: int
    REBALANCE_IN_PROGRESS: int
    REBOOTSTRAP_REQUIRED: int
    RECORD_LIST_TOO_LARGE: int
    REPLICA_NOT_AVAILABLE: int
    REQUEST_TIMED_OUT: int
    RESOURCE_NOT_FOUND: int
    SASL_AUTHENTICATION_FAILED: int
    SECURITY_DISABLED: int
    SHARE_SESSION_LIMIT_REACHED: int
    SHARE_SESSION_NOT_FOUND: int
    STALE_BROKER_EPOCH: int
    STALE_CTRL_EPOCH: int
    STALE_MEMBER_EPOCH: int
    STREAMS_INVALID_TOPOLOGY: int
    STREAMS_INVALID_TOPOLOGY_EPOCH: int
    STREAMS_TOPOLOGY_FENCED: int
    TELEMETRY_TOO_LARGE: int
    THROTTLING_QUOTA_EXCEEDED: int
    TOPIC_ALREADY_EXISTS: int
    TOPIC_AUTHORIZATION_FAILED: int
    TOPIC_DELETION_DISABLED: int
    TOPIC_EXCEPTION: int
    TRANSACTION_ABORTABLE: int
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED: int
    TRANSACTION_COORDINATOR_FENCED: int
    UNACCEPTABLE_CREDENTIAL: int
    UNKNOWN: int
    UNKNOWN_CONTROLLER_ID: int
    UNKNOWN_LEADER_EPOCH: int
    UNKNOWN_MEMBER_ID: int
    UNKNOWN_PRODUCER_ID: int
    UNKNOWN_SUBSCRIPTION_ID: int
    UNKNOWN_TOPIC_ID: int
    UNKNOWN_TOPIC_OR_PART: int
    UNRELEASED_INSTANCE_ID: int
    UNSTABLE_OFFSET_COMMIT: int
    UNSUPPORTED_ASSIGNOR: int
    UNSUPPORTED_COMPRESSION_TYPE: int
    UNSUPPORTED_ENDPOINT_TYPE: int
    UNSUPPORTED_FOR_MESSAGE_FORMAT: int
    UNSUPPORTED_SASL_MECHANISM: int
    UNSUPPORTED_VERSION: int
    VOTER_NOT_FOUND: int
    _ALL_BROKERS_DOWN: int
    _APPLICATION: int
    _ASSIGNMENT_LOST: int
    _ASSIGN_PARTITIONS: int
    _AUTHENTICATION: int
    _AUTO_OFFSET_RESET: int
    _BAD_COMPRESSION: int
    _BAD_MSG: int
    _CONFLICT: int
    _CRIT_SYS_RESOURCE: int
    _DESTROY: int
    _DESTROY_BROKER: int
    _EXISTING_SUBSCRIPTION: int
    _FAIL: int
    _FATAL: int
    _FENCED: int
    _FS: int
    _GAPLESS_GUARANTEE: int
    _INCONSISTENT: int
    _INTR: int
    _INVALID_ARG: int
    _INVALID_DIFFERENT_RECORD: int
    _INVALID_TYPE: int
    _IN_PROGRESS: int
    _ISR_INSUFF: int
    _KEY_DESERIALIZATION: int
    _KEY_SERIALIZATION: int
    _LOG_TRUNCATION: int
    _MAX_POLL_EXCEEDED: int
    _MSG_TIMED_OUT: int
    _NODE_UPDATE: int
    _NOENT: int
    _NOOP: int
    _NOT_CONFIGURED: int
    _NOT_IMPLEMENTED: int
    _NO_OFFSET: int
    _OUTDATED: int
    _PARTIAL: int
    _PARTITION_EOF: int
    _PREV_IN_PROGRESS: int
    _PURGE_INFLIGHT: int
    _PURGE_QUEUE: int
    _QUEUE_FULL: int
    _READ_ONLY: int
    _RESOLVE: int
    _RETRY: int
    _REVOKE_PARTITIONS: int
    _SSL: int
    _STATE: int
    _TIMED_OUT: int
    _TIMED_OUT_QUEUE: int
    _TRANSPORT: int
    _UNDERFLOW: int
    _UNKNOWN_BROKER: int
    _UNKNOWN_GROUP: int
    _UNKNOWN_PARTITION: int
    _UNKNOWN_PROTOCOL: int
    _UNKNOWN_TOPIC: int
    _UNSUPPORTED_FEATURE: int
    _VALUE_DESERIALIZATION: int
    _VALUE_SERIALIZATION: int
    _WAIT_CACHE: int
    _WAIT_COORD: int
    def __init__(
        self,
        code: int,
        reason: Optional[str] = None,
        fatal: bool = False,
        retriable: bool = False,
        txn_requires_abort: bool = False,
    ) -> None: ...
    def code(self) -> int: ...
    def name(self) -> builtins.str: ...
    def str(self) -> builtins.str: ...
    def fatal(self) -> bool: ...
    def retriable(self) -> bool: ...
    def txn_requires_abort(self) -> bool: ...
    def __str__(self) -> builtins.str: ...
    def __bool__(self) -> bool: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: Union['KafkaError', int]) -> bool: ...
    def __le__(self, other: Union['KafkaError', int]) -> bool: ...
    def __gt__(self, other: Union['KafkaError', int]) -> bool: ...
    def __ge__(self, other: Union['KafkaError', int]) -> bool: ...

class KafkaException(Exception):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    args: Tuple[Any, ...]

# These subclass RuntimeError; str(exc) is the error message (not a KafkaError).
class IllegalStateException(RuntimeError):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    args: Tuple[Any, ...]

class ConcurrentModificationException(RuntimeError):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    args: Tuple[Any, ...]

class Message:
    def __init__(
        self,
        topic: Optional[str] = ...,
        partition: Optional[int] = ...,
        offset: Optional[int] = ...,
        key: Optional[bytes] = ...,
        value: Optional[bytes] = ...,
        headers: Optional[HeadersType] = ...,
        error: Optional[KafkaError] = ...,
        timestamp: Optional[Tuple[int, int]] = ...,
        latency: Optional[float] = ...,
        leader_epoch: Optional[int] = ...,
        delivery_count: Optional[int] = ...,
    ) -> None: ...
    def topic(self) -> Optional[str]: ...
    def partition(self) -> Optional[int]: ...
    def offset(self) -> Optional[int]: ...
    def key(self) -> Optional[bytes]: ...
    def value(self) -> Optional[bytes]: ...
    def headers(self) -> Optional[HeadersType]: ...
    def error(self) -> Optional[KafkaError]: ...
    def timestamp(self) -> Tuple[int, int]: ...  # (timestamp_type, timestamp)
    def latency(self) -> Optional[float]: ...
    def leader_epoch(self) -> Optional[int]: ...
    def delivery_count(self) -> Optional[int]: ...
    def set_headers(self, headers: HeadersType) -> None: ...
    def set_key(self, key: Any) -> None: ...
    def set_value(self, value: Any) -> None: ...
    def set_error(self, error: Optional[KafkaError]) -> None: ...
    def __len__(self) -> int: ...

class TopicPartition:
    def __init__(
        self,
        topic: str,
        partition: int = -1,
        offset: int = -1001,
        metadata: Optional[str] = None,
        leader_epoch: Optional[int] = None,
    ) -> None: ...
    topic: str
    partition: int
    offset: int
    leader_epoch: int
    metadata: Optional[str]
    error: Optional[KafkaError]
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __lt__(self, other: 'TopicPartition') -> bool: ...

class Uuid:
    def __init__(self, most_significant_bits: int, least_significant_bits: int) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def __int__(self) -> int: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...

class Producer:
    @overload
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Create Producer with configuration dict.

        Args:
            config: Configuration dictionary. Typically includes 'bootstrap.servers'.
                    Can also include callbacks (error_cb, stats_cb, etc.)

        Example:
            Producer({'bootstrap.servers': 'localhost:9092'})
        """
        ...

    @overload
    def __init__(self, config: Dict[str, Any], /, **kwargs: Any) -> None:
        """
        Create Producer with configuration dict and additional keyword arguments.
        Keyword arguments override values in the config dict.

        Args:
            config: Configuration dictionary.
            **kwargs: Additional config as keyword args (overrides dict values).

        Example:
            Producer(conf, logger=logger)
            Producer({'bootstrap.servers': 'localhost'}, enable_idempotence=True)
        """
        ...

    @overload
    def __init__(self, **config: Any) -> None:
        """
        Create Producer with keyword arguments only.

        Args:
            **config: Configuration as keyword args.
                      Note: Use underscores (bootstrap_servers) not dots (bootstrap.servers) in kwargs.

        Example:
            Producer(bootstrap_servers='localhost:9092')
        """
        ...

    def produce(
        self,
        topic: str,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        partition: int = -1,
        callback: Optional[DeliveryCallback] = None,
        on_delivery: Optional[DeliveryCallback] = None,
        timestamp: int = 0,
        headers: Optional[HeadersType] = None,
    ) -> None: ...
    def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        partition: int = -1,
        callback: Optional[DeliveryCallback] = None,
        on_delivery: Optional[DeliveryCallback] = None,
    ) -> int: ...
    def poll(self, timeout: float = -1) -> int: ...
    def flush(self, timeout: float = -1) -> int: ...
    def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None: ...
    def abort_transaction(self, timeout: float = -1) -> None: ...
    def begin_transaction(self) -> None: ...
    def commit_transaction(self, timeout: float = -1) -> None: ...
    def init_transactions(self, timeout: float = -1) -> None: ...
    def send_offsets_to_transaction(
        self, positions: List[TopicPartition], group_metadata: Any, timeout: float = -1  # ConsumerGroupMetadata
    ) -> None: ...
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> Any: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> Optional[bool]: ...

class Consumer:
    @overload
    def __init__(self, config: dict[str, Any]) -> None:
        """
        Create Consumer with configuration dict.

        Args:
            config: Configuration dictionary. Must include 'group.id'.
                    Can also include callbacks (error_cb, stats_cb, etc.)

        Example:
            Consumer({'bootstrap.servers': 'localhost', 'group.id': 'mygroup'})
        """
        ...

    @overload
    def __init__(self, config: dict[str, Any], /, **kwargs: Any) -> None:
        """
        Create Consumer with configuration dict and additional keyword arguments.
        Keyword arguments override values in the config dict.

        Args:
            config: Configuration dictionary. Must include 'group.id'.
            **kwargs: Additional config as keyword args (overrides dict values).

        Example:
            Consumer(conf, logger=logger)
            Consumer({'bootstrap.servers': 'localhost'}, group_id='mygroup')
        """
        ...

    @overload
    def __init__(self, **config: Any) -> None:
        """
        Create Consumer with keyword arguments only.

        Args:
            **config: Configuration as keyword args. Must include group_id.
                      Note: Use underscores (group_id) not dots (group.id) in kwargs.

        Example:
            Consumer(bootstrap_servers='localhost', group_id='mygroup')
        """
        ...

    def subscribe(
        self,
        topics: List[str],
        on_assign: Optional[RebalanceCallback] = None,
        on_revoke: Optional[RebalanceCallback] = None,
        on_lost: Optional[RebalanceCallback] = None,
    ) -> None: ...
    def unsubscribe(self) -> None: ...
    def poll(self, timeout: float = -1) -> Optional[Message]: ...
    def consume(self, num_messages: int = 1, timeout: float = -1) -> List[Message]: ...
    def assign(self, partitions: List[TopicPartition]) -> None: ...
    def unassign(self) -> None: ...
    def assignment(self) -> List[TopicPartition]: ...
    @overload
    def commit(
        self,
        *,
        asynchronous: Literal[True] = ...,
    ) -> None:
        """
        Message and offsets omitted, asynchronous.
        """
        ...

    @overload
    def commit(
        self,
        *,
        asynchronous: Literal[False],
    ) -> List[TopicPartition]:
        """
        Message and offsets omitted, synchronous.
        """
        ...

    @overload
    def commit(
        self,
        *,
        message: Message,
        asynchronous: Literal[True] = ...,
    ) -> None:
        """
        Message specified, asynchronous.
        """
        ...

    @overload
    def commit(
        self,
        *,
        message: Message,
        asynchronous: Literal[False],
    ) -> List[TopicPartition]:
        """
        Message specified, synchronous.
        """
        ...

    @overload
    def commit(
        self,
        *,
        offsets: List[TopicPartition],
        asynchronous: Literal[True] = ...,
    ) -> None:
        """
        Offsets specified, asynchronous.
        """
        ...

    @overload
    def commit(
        self,
        *,
        offsets: List[TopicPartition],
        asynchronous: Literal[False],
    ) -> List[TopicPartition]:
        """
        Offsets specified, synchronous
        """
        ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> Tuple[int, int]: ...
    def pause(self, partitions: List[TopicPartition]) -> None: ...
    def resume(self, partitions: List[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]: ...
    def store_offsets(
        self, message: Optional['Message'] = None, offsets: Optional[List[TopicPartition]] = None
    ) -> None: ...
    def committed(self, partitions: List[TopicPartition], timeout: float = -1) -> List[TopicPartition]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> Optional[bool]: ...
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> Any: ...
    def offsets_for_times(self, partitions: List[TopicPartition], timeout: float = -1) -> List[TopicPartition]: ...
    def incremental_assign(self, partitions: List[TopicPartition]) -> None: ...
    def incremental_unassign(self, partitions: List[TopicPartition]) -> None: ...
    def consumer_group_metadata(self) -> Any: ...  # ConsumerGroupMetadata
    def memberid(self) -> str: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...

class ShareConsumer:
    """Share Consumer for queue-like message consumption (KIP-932)."""

    @overload
    def __init__(self, config: Dict[str, Any]) -> None: ...
    @overload
    def __init__(self, config: Dict[str, Any], /, **kwargs: Any) -> None: ...
    @overload
    def __init__(self, **config: Any) -> None: ...
    def subscribe(self, topics: List[str]) -> None: ...
    def unsubscribe(self) -> None: ...
    def subscription(self) -> List[str]: ...
    def poll(self, timeout: float = -1) -> Messages: ...
    def acknowledge(self, message: Message, ack_type: AcknowledgeType = ...) -> None: ...
    def acknowledge_offset(
        self, topic: str, partition: int, offset: int, ack_type: AcknowledgeType = ...
    ) -> None: ...
    # TODO KIP-932: commit_sync() is keyed by the existing TopicPartition
    # (no UUID) for now. A future TopicIdPartition (topic name + topic UUID
    # + partition) would carry the UUID; add that class once the interface
    # is finalized.
    def commit_sync(self, timeout: float = 60) -> Dict[TopicPartition, Optional[KafkaException]]: ...
    def commit_async(self) -> None: ...
    def set_acknowledgement_commit_callback(
        self, callback: Optional[AcknowledgementCommitCallback]
    ) -> None: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "ShareConsumer": ...
    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> Optional[bool]: ...

class _AdminClientImpl:
    def __init__(self, config: Dict[str, Union[str, int, float, bool]]) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> Optional[bool]: ...
    def create_topics(
        self,
        topics: List['NewTopic'],
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        operation_timeout: float = -1,
    ) -> None: ...
    def delete_topics(
        self, topics: List[str], future: Any, request_timeout: float = -1, operation_timeout: float = -1
    ) -> None: ...
    def create_partitions(
        self,
        topics: List['NewPartitions'],
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        operation_timeout: float = -1,
    ) -> None: ...
    def describe_topics(
        self,
        future: Any,
        topic_names: List[str],
        request_timeout: float = -1,
        include_authorized_operations: bool = False,
    ) -> None: ...
    def describe_cluster(
        self, future: Any, request_timeout: float = -1, include_authorized_operations: bool = False
    ) -> None: ...
    def list_topics(self, topic: Optional[str] = None, timeout: float = -1) -> ClusterMetadata: ...
    def list_groups(self, group: Optional[str] = None, timeout: float = -1) -> List[GroupMetadata]: ...
    def describe_consumer_groups(
        self,
        group_ids: List[str],
        future: Any,
        request_timeout: float = -1,
        include_authorized_operations: bool = False,
    ) -> None: ...
    def list_consumer_groups(
        self,
        future: Any,
        states_int: Optional[List[int]] = None,
        types_int: Optional[List[int]] = None,
        request_timeout: float = -1,
    ) -> None: ...
    def list_consumer_group_offsets(
        self,
        request: Any,  # ConsumerGroupTopicPartitions
        future: Any,
        require_stable: bool = False,
        request_timeout: float = -1,
    ) -> None: ...
    def alter_consumer_group_offsets(
        self, requests: Any, future: Any, request_timeout: float = -1  # List[ConsumerGroupTopicPartitions]
    ) -> None: ...
    def delete_consumer_groups(self, group_ids: List[str], future: Any, request_timeout: float = -1) -> None: ...
    def create_acls(self, acls: List[Any], future: Any, request_timeout: float = -1) -> None: ...  # List[AclBinding]
    def describe_acls(
        self, acl_binding_filter: Any, future: Any, request_timeout: float = -1  # AclBindingFilter
    ) -> None: ...
    def delete_acls(
        self, acls: List[Any], future: Any, request_timeout: float = -1  # List[AclBindingFilter]
    ) -> None: ...
    def describe_configs(
        self, resources: List[Any], future: Any, request_timeout: float = -1, broker: int = -1  # List[ConfigResource]
    ) -> None: ...
    def alter_configs(
        self,
        resources: List[Any],  # List[ConfigResource]
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        broker: int = -1,
    ) -> None: ...
    def incremental_alter_configs(
        self,
        resources: List[Any],  # List[ConfigResource]
        future: Any,
        validate_only: bool = False,
        request_timeout: float = -1,
        broker: int = -1,
    ) -> None: ...
    def describe_user_scram_credentials(
        self, users: Optional[List[str]], future: Any, request_timeout: float = -1
    ) -> None: ...
    def alter_user_scram_credentials(
        self, alterations: List[Any], future: Any, request_timeout: float = -1  # List[UserScramCredentialAlteration]
    ) -> None: ...
    def list_offsets(
        self,
        topic_partitions: List[TopicPartition],
        future: Any,
        isolation_level_value: Optional[int] = None,
        request_timeout: float = -1,
    ) -> None: ...
    def delete_records(
        self,
        topic_partition_offsets: List[TopicPartition],
        future: Any,
        request_timeout: float = -1,
        operation_timeout: float = -1,
    ) -> None: ...
    def elect_leaders(
        self,
        election_type: int,
        partitions: Optional[List[TopicPartition]],
        future: Any,
        request_timeout: float = -1,
        operation_timeout: float = -1,
    ) -> None: ...
    def poll(self, timeout: float = -1) -> int: ...
    def set_sasl_credentials(self, username: str, password: str) -> None: ...

class NewTopic:
    def __init__(
        self,
        topic: str,
        num_partitions: int = -1,
        replication_factor: int = -1,
        replica_assignment: Optional[List[List[int]]] = None,
        config: Optional[Dict[str, str]] = None,
    ) -> None: ...
    topic: str
    num_partitions: int
    replication_factor: int
    replica_assignment: Optional[List[List[int]]]
    config: Optional[Dict[str, str]]
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: 'NewTopic') -> bool: ...
    def __le__(self, other: 'NewTopic') -> bool: ...
    def __gt__(self, other: 'NewTopic') -> bool: ...
    def __ge__(self, other: 'NewTopic') -> bool: ...

class NewPartitions:
    def __init__(
        self, topic: str, new_total_count: int, replica_assignment: Optional[List[List[int]]] = None
    ) -> None: ...
    topic: str
    new_total_count: int
    replica_assignment: Optional[List[List[int]]]
    def __str__(self) -> str: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: 'NewPartitions') -> bool: ...
    def __le__(self, other: 'NewPartitions') -> bool: ...
    def __gt__(self, other: 'NewPartitions') -> bool: ...
    def __ge__(self, other: 'NewPartitions') -> bool: ...

# ===== MODULE FUNCTIONS (From stubgen) =====

def libversion() -> Tuple[str, int]: ...
def version() -> Tuple[str, int]: ...
def murmur2(key: bytes, partition_count: int) -> int: ...
def consistent(key: bytes, partition_count: int) -> int: ...
def fnv1a(key: bytes, partition_count: int) -> int: ...

# ===== CONSTANTS (From stubgen) =====

ACL_OPERATION_ALL: int
ACL_OPERATION_ALTER: int
ACL_OPERATION_ALTER_CONFIGS: int
ACL_OPERATION_ANY: int
ACL_OPERATION_CLUSTER_ACTION: int
ACL_OPERATION_CREATE: int
ACL_OPERATION_DELETE: int
ACL_OPERATION_DESCRIBE: int
ACL_OPERATION_DESCRIBE_CONFIGS: int
ACL_OPERATION_IDEMPOTENT_WRITE: int
ACL_OPERATION_READ: int
ACL_OPERATION_UNKNOWN: int
ACL_OPERATION_WRITE: int
ACL_PERMISSION_TYPE_ALLOW: int
ACL_PERMISSION_TYPE_ANY: int
ACL_PERMISSION_TYPE_DENY: int
ACL_PERMISSION_TYPE_UNKNOWN: int
ALTER_CONFIG_OP_TYPE_APPEND: int
ALTER_CONFIG_OP_TYPE_DELETE: int
ALTER_CONFIG_OP_TYPE_SET: int
ALTER_CONFIG_OP_TYPE_SUBTRACT: int
CONFIG_SOURCE_DEFAULT_CONFIG: int
CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG: int
CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG: int
CONFIG_SOURCE_GROUP_CONFIG: int
CONFIG_SOURCE_STATIC_BROKER_CONFIG: int
CONFIG_SOURCE_UNKNOWN_CONFIG: int
CONSUMER_GROUP_STATE_COMPLETING_REBALANCE: int
CONSUMER_GROUP_STATE_DEAD: int
CONSUMER_GROUP_STATE_EMPTY: int
CONSUMER_GROUP_STATE_PREPARING_REBALANCE: int
CONSUMER_GROUP_STATE_STABLE: int
CONSUMER_GROUP_STATE_UNKNOWN: int
CONSUMER_GROUP_TYPE_CLASSIC: int
CONSUMER_GROUP_TYPE_CONSUMER: int
CONSUMER_GROUP_TYPE_UNKNOWN: int
ELECTION_TYPE_PREFERRED: int
ELECTION_TYPE_UNCLEAN: int
ISOLATION_LEVEL_READ_COMMITTED: int
ISOLATION_LEVEL_READ_UNCOMMITTED: int
OFFSET_BEGINNING: int
OFFSET_END: int
OFFSET_INVALID: int
OFFSET_SPEC_EARLIEST: int
OFFSET_SPEC_LATEST: int
OFFSET_SPEC_MAX_TIMESTAMP: int
OFFSET_STORED: int
SHARE_ACKNOWLEDGE_TYPE_ACCEPT: int
SHARE_ACKNOWLEDGE_TYPE_RELEASE: int
SHARE_ACKNOWLEDGE_TYPE_REJECT: int
RESOURCE_ANY: int
RESOURCE_BROKER: int
RESOURCE_GROUP: int
RESOURCE_PATTERN_ANY: int
RESOURCE_PATTERN_LITERAL: int
RESOURCE_PATTERN_MATCH: int
RESOURCE_PATTERN_PREFIXED: int
RESOURCE_PATTERN_UNKNOWN: int
RESOURCE_TOPIC: int
RESOURCE_TRANSACTIONAL_ID: int
RESOURCE_UNKNOWN: int
SCRAM_MECHANISM_SHA_256: int
SCRAM_MECHANISM_SHA_512: int
SCRAM_MECHANISM_UNKNOWN: int
TIMESTAMP_CREATE_TIME: int
TIMESTAMP_LOG_APPEND_TIME: int
TIMESTAMP_NOT_AVAILABLE: int
