LIBRARY()

SRCS(
    quoter_service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/protos
    contrib/ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
