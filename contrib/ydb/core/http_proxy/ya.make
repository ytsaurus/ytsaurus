LIBRARY()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    auth_factory.cpp
    auth_factory.h
    auth_actors.cpp
    auth_actors.h
    custom_metrics.h
    discovery_actor.cpp
    discovery_actor.h
    events.h
    exceptions_mapping.cpp
    exceptions_mapping.h
    grpc_service.cpp
    grpc_service.h
    http_req.cpp
    http_req.h
    http_service.cpp
    http_service.h
    json_proto_conversion.h
    metrics_actor.cpp
    metrics_actor.h
)

PEERDIR(
    contrib/libs/grpc
    contrib/restricted/nlohmann_json
    contrib/ydb/library/actors/http
    contrib/ydb/library/actors/core
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/security
    yql/essentials/public/issue
    contrib/ydb/library/http_proxy/authorization
    contrib/ydb/library/http_proxy/error
    contrib/ydb/library/ycloud/api
    contrib/ydb/library/ycloud/impl
    contrib/ydb/library/naming_conventions
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
    contrib/ydb/public/sdk/cpp/src/client/iam_private
    contrib/ydb/services/datastreams
    contrib/ydb/services/datastreams/codes
    contrib/ydb/services/persqueue_v1/actors
    contrib/ydb/services/sqs_topic
    contrib/ydb/services/sqs_topic/queue_url
    contrib/ydb/services/ymq
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
