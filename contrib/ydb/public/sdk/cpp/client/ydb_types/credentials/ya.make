LIBRARY()

SRCS(
    credentials.cpp
)

PEERDIR(
    contrib/ydb/library/login
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
    contrib/ydb/library/yql/public/issue
)

END()
