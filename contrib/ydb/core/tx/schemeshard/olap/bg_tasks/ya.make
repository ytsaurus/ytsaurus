LIBRARY()

PEERDIR(
    contrib/ydb/core/tx/columnshard/bg_tasks
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/adapter
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/protos
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/events
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/transactions
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain
)

END()
