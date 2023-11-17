LIBRARY()

SRCS(
    iam_impl.h
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/http/simple
    library/cpp/json
    library/cpp/threading/atomic
    contrib/ydb/public/lib/jwt
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/public/sdk/cpp/client/iam/common
)

END()
