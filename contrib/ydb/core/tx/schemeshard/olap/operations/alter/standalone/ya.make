LIBRARY()

SRCS(
    object.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/core/tx/schemeshard/olap/schema
    contrib/ydb/core/tx/schemeshard/olap/ttl
    contrib/ydb/core/tx/schemeshard/olap/table
)

YQL_LAST_ABI_VERSION()

END()
