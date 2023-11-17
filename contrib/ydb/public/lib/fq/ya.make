LIBRARY()

SRCS(
    fq.cpp
    scope.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

END()
