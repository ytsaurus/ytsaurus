LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain
)

YQL_LAST_ABI_VERSION()

END()
