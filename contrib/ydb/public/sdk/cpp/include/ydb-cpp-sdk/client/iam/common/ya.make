LIBRARY(client-iam-common-include)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    types.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/jwt
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
