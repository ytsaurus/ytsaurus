LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/manager
    contrib/ydb/core/tx/columnshard/blobs_action/bs
    contrib/ydb/core/tx/columnshard/blobs_action/local
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
