LIBRARY()

SRCS(
    gc.cpp
    storage_proxy.cpp
    storage_service.cpp
    storage_settings.cpp
    ydb_checkpoint_storage.cpp
    ydb_state_storage.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/fq/libs/checkpoint_storage/events
    contrib/ydb/core/fq/libs/checkpoint_storage/proto
    contrib/ydb/core/fq/libs/checkpointing_common
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/library/security
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
