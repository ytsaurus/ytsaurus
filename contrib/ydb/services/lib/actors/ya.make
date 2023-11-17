LIBRARY()

SRCS(
    pq_schema_actor.cpp
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/digest/md5
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/metering
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/library/persqueue/obfuscate
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/lib/jwt
    contrib/ydb/public/lib/operation_id
)

END()
