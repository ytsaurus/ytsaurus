LIBRARY()

SRCS(
    consumers_advanced_monitoring_settings.cpp
    pq_schema_actor.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/json
    library/cpp/digest/md5
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/metering
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/util
    contrib/ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/library/jwt
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

END()
