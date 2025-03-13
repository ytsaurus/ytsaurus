LIBRARY(client-iam-common-include)

SRCS(
    types.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/jwt
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
