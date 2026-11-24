LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    credentials.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/types/status
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()

RECURSE(
    oauth2_token_exchange
    oidc
)
