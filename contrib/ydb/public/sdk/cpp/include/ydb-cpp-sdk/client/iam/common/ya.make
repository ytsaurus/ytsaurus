LIBRARY(client-iam-common-include)

SRCS(
    types.h
)

PEERDIR(
    contrib/libs/grpc
    contrib/ydb/public/sdk/cpp/src/library/issue
    contrib/ydb/public/sdk/cpp/src/library/jwt
    contrib/ydb/public/sdk/cpp/src/library/time
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
    contrib/ydb/public/sdk/cpp/src/client/types/status
)

END()
