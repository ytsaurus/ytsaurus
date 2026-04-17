LIBRARY()

SRCS(
    rate_limiter_control_plane_service.cpp
    update_limit_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/rate_limiter/utils
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/protos
    contrib/ydb/library/security
    contrib/ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()
