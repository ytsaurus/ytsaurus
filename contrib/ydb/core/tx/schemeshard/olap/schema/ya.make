LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/column_families
    contrib/ydb/core/tx/schemeshard/olap/columns
    contrib/ydb/core/tx/schemeshard/olap/indexes
    contrib/ydb/core/tx/schemeshard/olap/options
    contrib/ydb/core/tx/schemeshard/common
)

END()
