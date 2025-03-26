LIBRARY()

SRCS(
    common.cpp
    manager.cpp
    fetcher.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/json/writer
    contrib/ydb/core/blobstorage
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tiering/tier
    contrib/ydb/core/tablet_flat/protos
    contrib/ydb/core/wrappers
    contrib/ydb/public/api/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)