LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL backup.cpp
    GLOBAL sharing.cpp
    GLOBAL restore.cpp
    propose_tx.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/backup/import
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/export/session
    contrib/ydb/core/tx/columnshard/transactions/operators/ev_write
)

END()
