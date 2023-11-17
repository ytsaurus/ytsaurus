LIBRARY()

SRCS(
    common.cpp
    tier_cleaner.cpp
    path_cleaner.cpp
    GLOBAL cleaner_task.cpp
    manager.cpp
    GLOBAL external_data.cpp
    snapshot.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

PEERDIR(
    library/cpp/actors/core
    library/cpp/json/writer
    contrib/ydb/core/blobstorage
    contrib/ydb/core/protos
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tiering/rule
    contrib/ydb/core/tx/tiering/tier
    contrib/ydb/core/tablet_flat/protos
    contrib/ydb/core/wrappers
    contrib/ydb/public/api/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/metadata
)

END()

RECURSE_FOR_TESTS(
    ut
)