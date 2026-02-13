LIBRARY()

SRCS(
    actor.cpp
    change_message_visibility.cpp
    error.cpp
    delete_message.cpp
    receipt.cpp
    receive_message.cpp
    send_message.cpp
    sqs_topic_proxy.cpp
    statuses.cpp
    utils.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/core/base
    contrib/ydb/core/client/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/services/datastreams/codes
    contrib/ydb/services/lib/actors
    contrib/ydb/services/lib/sharding
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/sqs_topic/queue_url
    contrib/ydb/services/sqs_topic/queue_url/holder
    contrib/ydb/services/sqs_topic/protos/receipt
    contrib/ydb/services/ydb
    contrib/ydb/core/persqueue/public/describer
    contrib/ydb/core/persqueue/public/mlp
    contrib/ydb/core/ymq/attributes
    contrib/ydb/core/ymq/base
    contrib/ydb/core/ymq/error
)

END()

RECURSE(
    protos
    queue_url
)
