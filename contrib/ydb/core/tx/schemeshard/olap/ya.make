LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/columns
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks
    contrib/ydb/core/tx/schemeshard/olap/indexes
    contrib/ydb/core/tx/schemeshard/olap/schema
    contrib/ydb/core/tx/schemeshard/olap/common
    contrib/ydb/core/tx/schemeshard/olap/operations
    contrib/ydb/core/tx/schemeshard/olap/options
    contrib/ydb/core/tx/schemeshard/olap/layout
    contrib/ydb/core/tx/schemeshard/olap/manager
    contrib/ydb/core/tx/schemeshard/olap/store
    contrib/ydb/core/tx/schemeshard/olap/table
    contrib/ydb/core/tx/schemeshard/olap/ttl
    contrib/ydb/core/tx/schemeshard/olap/column_families
)

END()
