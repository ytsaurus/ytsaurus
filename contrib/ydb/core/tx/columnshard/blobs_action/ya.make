LIBRARY()

SRCS(
    blob_manager_db.cpp
    memory.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/bs
    contrib/ydb/core/tx/columnshard/blobs_action/counters
    contrib/ydb/core/tx/columnshard/blobs_action/transaction
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    PEERDIR(
        contrib/ydb/core/tx/columnshard/blobs_action/tier
    )
ENDIF()

END()
