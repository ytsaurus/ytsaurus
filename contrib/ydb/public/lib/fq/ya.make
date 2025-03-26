LIBRARY()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    fq.cpp
    scope.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
