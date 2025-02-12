LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    credentials.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()

RECURSE(
    oauth2_token_exchange
)
