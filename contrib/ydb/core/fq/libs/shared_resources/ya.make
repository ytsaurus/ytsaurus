LIBRARY()

SRCS(
    db_exec.cpp
    shared_resources.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/exceptions
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/shared_resources/interface
    contrib/ydb/core/protos
    contrib/ydb/library/db_pool
    contrib/ydb/library/logger
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    interface
)
