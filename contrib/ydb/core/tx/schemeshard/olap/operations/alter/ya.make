LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/common
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/in_store
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/standalone
)

YQL_LAST_ABI_VERSION()

END()
