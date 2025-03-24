LIBRARY()

SRCS(
    GLOBAL secondary.cpp
    GLOBAL simple.cpp
    GLOBAL primary.cpp
    abstract.cpp
    sync.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/events
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/transactions/locks
)

END()
