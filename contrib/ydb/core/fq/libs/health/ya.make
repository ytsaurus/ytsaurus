LIBRARY()

SRCS(
    health.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/mon
    contrib/ydb/public/sdk/cpp/src/client/discovery
)

YQL_LAST_ABI_VERSION()

END()
