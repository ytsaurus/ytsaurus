LIBRARY()

SRCS(
    http_req.cpp
    http_req.h
    http_router.cpp
    http_router.h
    http_service.cpp
    http_service.h
    grpc_request_context_wrapper.cpp
    grpc_request_context_wrapper.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    library/cpp/protobuf/json
    library/cpp/resource
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/result_formatter
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/http_proxy
    contrib/ydb/core/protos
    contrib/ydb/core/public_http/protos
    contrib/ydb/core/viewer/json
    yql/essentials/public/issue
    contrib/ydb/public/sdk/cpp/src/client/types
)

RESOURCE(
    openapi/openapi.yaml resources/openapi.yaml
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
