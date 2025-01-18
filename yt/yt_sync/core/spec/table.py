from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import auto
from enum import StrEnum
from typing import Any
from typing import Optional
from typing import TypeAlias
from typing import Union

from yt.yson.yson_types import YsonBoolean


@dataclass
class Table:
    """
    Specification for YT table federation: chaotic, replicated, single.

    For details see:
    - https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/overview
    - https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/replicated-dynamic-tables
    """

    class Type(StrEnum):
        REPLICATED_TABLE = "replicated_table"
        CHAOS_REPLICATED_TABLE = "chaos_replicated_table"
        TABLE = "table"
        REPLICATION_LOG = "replication_log_table"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    chaos: Optional[bool] = None
    """
    Defines if specification describes chaos federation instead of replicated.

    If omitted then evaluated as ``False``.
    """

    in_collocation: Optional[bool] = None
    """Defines if replicated/chaotic table of table federation is in collocation."""

    schema: Optional[list[Column]] = None
    """Schema for all tables in federation.

    Empty or absent schema is not allowed.

    See :py:class:`Column` for details.
    """

    clusters: Optional[dict[str, ClusterTable]] = None
    """
    Specifications for table on each cluster of table federation.
    At least one cluster must be present.

    If only one cluster is present then table treated as single.
    For several clusters one should be mandatory marked as main, in that case table treated as replicated or chaotic.

    See :py:class:`ClusterTable` for details.
    """


class TypeV1(StrEnum):
    """Available types for column.

    See https://ytsaurus.tech/docs/ru/user-guide/storage/data-types for details.
    """

    INT64 = auto()
    INT32 = auto()
    INT16 = auto()
    INT8 = auto()
    UINT64 = auto()
    UINT32 = auto()
    UINT16 = auto()
    UINT8 = auto()
    DOUBLE = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    STRING = auto()
    UTF8 = auto()
    JSON = auto()
    UUID = auto()
    DATE = auto()
    DATETIME = auto()
    TIMESTAMP = auto()
    INTERVAL = auto()
    ANY = auto()
    VOID = auto()


@dataclass
class TypeV3:
    """Specification for type_v3.

    See https://ytsaurus.tech/docs/ru/user-guide/storage/data-types for details.
    """

    class Type(StrEnum):
        INT64 = auto()
        INT32 = auto()
        INT16 = auto()
        INT8 = auto()
        UINT64 = auto()
        UINT32 = auto()
        UINT16 = auto()
        UINT8 = auto()
        DOUBLE = auto()
        FLOAT = auto()
        BOOL = auto()
        STRING = auto()
        UTF8 = auto()
        JSON = auto()
        UUID = auto()
        DATE = auto()
        DATETIME = auto()
        TIMESTAMP = auto()
        INTERVAL = auto()
        YSON = auto()
        DECIMAL = auto()
        OPTIONAL = auto()
        LIST = auto()
        STRUCT = auto()
        TUPLE = auto()
        VARIANT = auto()
        DICT = auto()
        TAGGED = auto()
        VOID = auto()

    @dataclass
    class TypeDecimal:
        type_name: TypeV3.Type
        precision: int
        scale: int

    @dataclass
    class TypeItem:
        # optional, list
        type_name: TypeV3.Type
        item: TypeV3Union

    @dataclass
    class TypeMembersOrElements:
        # tuple, struct, variant
        @dataclass
        class Member:
            name: Optional[str]
            type: TypeV3Union

        type_name: TypeV3.Type
        members: list[Member] = dataclass_field(default_factory=list)
        elements: list[Member] = dataclass_field(default_factory=list)

    @dataclass
    class TypeDict:
        type_name: TypeV3.Type
        key: TypeV3.Type
        value: TypeV3Union

    @dataclass
    class TypeTagged:
        type_name: TypeV3.Type
        tag: str
        item: TypeV3Union


# DO NOT CHANGE ORDER!!!
TypeV3Union: TypeAlias = Union[
    TypeV3.Type,
    TypeV3.TypeDecimal,
    TypeV3.TypeTagged,
    TypeV3.TypeItem,
    TypeV3.TypeDict,
    TypeV3.TypeMembersOrElements,
]


@dataclass
class Column:
    """Specification for YT table schema column.

    See for details:
    - https://ytsaurus.tech/docs/ru/user-guide/storage/static-schema#schema_overview
    - https://ytsaurus.tech/docs/ru/user-guide/storage/data-types#schema_primitive
    """

    class SortOrder(StrEnum):
        ASCENDING = auto()
        DESCENDING = auto()

    class AggregateFunc(StrEnum):
        SUM = auto()
        MIN = auto()
        MAX = auto()
        FIRST = auto()
        XDELTA = auto()
        DICT_SUM = auto()

    name: Optional[str] = None
    """ Column name. Mandatory attribute."""

    type: Optional[TypeV1] = None
    """Column type.

    Mandatory attributes are type or type_v3 (or both).

    See https://ytsaurus.tech/docs/ru/user-guide/storage/data-types for details."""

    type_v3: Optional[TypeV3Union] = None
    """Column type in type_v3 format.

    Mandatory attributes are type or type_v3 (or both).

    See https://ytsaurus.tech/docs/ru/user-guide/storage/data-types for details."""

    sort_order: Optional[SortOrder] = None
    """Column sort order. Use of descending is not recommended (YT works bad with it)."""

    expression: Optional[str] = None
    """Expression for computed column.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/resharding#expression for details."""

    required: Optional[Union[bool, YsonBoolean]] = None
    """Marks column as required. See https://ytsaurus.tech/docs/ru/user-guide/storage/data-types for details."""

    lock: Optional[str] = None
    """Transaction lock group. Only for non-key columns."""

    group: Optional[str] = None
    """Column data group."""

    aggregate: Optional[str] = None
    """Aggregate function for aggregated column.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/sorted-dynamic-tables#aggr_columns for details."""

    max_inline_hunk_size: Optional[int] = None
    """Configures hunks for column."""


@dataclass
class ReplicationLog:
    """Specification for chaos ``replication_log`` tables."""

    path: Optional[str] = None
    """Path of table. Can be defined explicitly or will be evaluated as ``{data_replica_table.path}_log``."""

    attributes: Optional[dict[str, Any]] = None
    """
    List of chaos ``replication_log`` table attributes.

    Several attributes are inherited from data replica table but can be overriden.

    Inherited attributes are:
    - ``tablet_cell_bundle``
    - ``primary_medium``
    - ``enable_replicated_table_tracker``
    """


@dataclass
class ClusterTable:
    """Specification for table of federation on specific cluster."""

    main: Optional[bool] = None
    """
    Defines if table on given cluster is replicated/chaotic or data replica.

    Must be consistent for all tables and nodes.
    May be omitted for single tables.

    ``main=True``
        ``replicated_table``/``chaos_replicated_table`` will be created
    ``main=False``
        ``table``/``replication_log`` will be created
    """

    path: Optional[str] = None
    """Path on specific cluster. Mandatory attribute."""

    schema_override: Optional[list[Column]] = None
    """Schema for table on given cluster.

    May be needed to add hunks, on given cluster, for example.

    See :py:class:`Column` for details.
    """

    replicated_table_tracker_enabled: Optional[bool] = None
    """Defines if given data replica is under RTT.

    Only for clusters with ``main=False``.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/replicated-dynamic-tables#repliki1 for details.
    """

    preferred_sync: Optional[bool] = None
    """
    Mark given data replica as sync/preferred sync (if RTT enabled).
    """

    attributes: Optional[dict[str, Any]] = None
    """Attributes of table on given cluster."""

    replication_log: Optional[ReplicationLog] = None
    """Replication log specification for chaotic table federations.

    See :py:class:`ReplicationLog` for details.
    """
