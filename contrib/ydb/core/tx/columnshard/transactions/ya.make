LIBRARY()

SRCS(
    tx_controller.cpp
    locks_db.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/data_events
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/transactions/operators
    contrib/ydb/core/tx/columnshard/transactions/transactions
    contrib/ydb/core/tx/columnshard/transactions/locks
)

YQL_LAST_ABI_VERSION()
GENERATE_ENUM_SERIALIZATION(tx_controller.h)

END()
