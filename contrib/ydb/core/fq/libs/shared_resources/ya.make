LIBRARY()

SRCS(
    db_exec.cpp
    shared_resources.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/shared_resources/interface
    contrib/ydb/core/protos
    contrib/ydb/library/db_pool
    contrib/ydb/library/logger
    contrib/ydb/library/security
    yql/essentials/utils
    contrib/ydb/public/sdk/cpp/src/client/extensions/solomon_stats
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/extension_common
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    interface
)
