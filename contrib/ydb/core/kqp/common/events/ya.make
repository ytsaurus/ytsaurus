LIBRARY()

SRCS(
    process_response.cpp
    events.cpp
    query.cpp
    script_executions.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/grpc_services/cancelation
    contrib/ydb/core/kqp/common/shutdown
    contrib/ydb/core/kqp/common/compilation

    contrib/ydb/library/yql/dq/actors
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/operation_id

    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
