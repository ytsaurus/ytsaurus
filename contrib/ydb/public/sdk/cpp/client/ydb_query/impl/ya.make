LIBRARY()

SRCS(
    exec_query.cpp
    exec_query.h
    client_session.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_proto
)

END()
