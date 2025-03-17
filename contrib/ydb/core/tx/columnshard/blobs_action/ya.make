LIBRARY()

SRCS(
    blob_manager_db.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/counters
    contrib/ydb/core/tx/columnshard/blobs_action/transaction
    contrib/ydb/core/tx/columnshard/blobs_action/events
    contrib/ydb/core/tx/columnshard/blobs_action/protos
)

END()
