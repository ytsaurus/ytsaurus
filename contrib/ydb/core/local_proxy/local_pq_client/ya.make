LIBRARY()

SRCS(
    local_topic_client_factory.cpp
    local_topic_client.cpp
    local_topic_read_session.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/kqp/common
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/gateway/clients/local
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/services/persqueue_v1/actors
    contrib/ydb/services/persqueue_v1
)

END()
