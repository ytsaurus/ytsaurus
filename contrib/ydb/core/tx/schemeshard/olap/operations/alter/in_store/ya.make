LIBRARY()

SRCS(
    object.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store/config_shards
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store/resharding
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store/schema
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store/transfer
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store/common
    contrib/ydb/core/tx/schemeshard/olap/ttl
    contrib/ydb/core/tx/schemeshard/olap/table
    contrib/ydb/core/tx/schemeshard/olap/store
    contrib/ydb/core/tx/schemeshard/olap/schema
)

YQL_LAST_ABI_VERSION()

END()
