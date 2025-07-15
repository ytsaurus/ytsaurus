LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL backup.cpp
    GLOBAL sharing.cpp
    propose_tx.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/transactions/operators/ev_write
    contrib/ydb/core/tx/columnshard/export/session
)

END()
