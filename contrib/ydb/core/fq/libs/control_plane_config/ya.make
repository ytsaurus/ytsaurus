LIBRARY()

SRCS(
    control_plane_config.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/control_plane_config/events
    contrib/ydb/core/fq/libs/quota_manager
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/mon
    contrib/ydb/library/db_pool
    contrib/ydb/library/security
    contrib/ydb/library/protobuf_printer
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)
