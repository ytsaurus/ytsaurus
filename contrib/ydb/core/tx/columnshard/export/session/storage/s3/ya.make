LIBRARY()

SRCS(
    GLOBAL storage.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/export/session/selector/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/wrappers
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
