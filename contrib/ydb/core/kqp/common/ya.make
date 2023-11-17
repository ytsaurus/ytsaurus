LIBRARY()

SRCS(
    kqp_event_ids.h
    kqp_event_impl.cpp
    kqp_resolve.cpp
    kqp_resolve.h
    kqp_ru_calc.cpp
    kqp_yql.cpp
    kqp_yql.h
    kqp_script_executions.cpp
    kqp_timeouts.h
    kqp_timeouts.cpp
    kqp_lwtrace_probes.h
    kqp_lwtrace_probes.cpp
    kqp_types.h
    kqp_types.cpp
    kqp.cpp
    kqp.h
    kqp_user_request_context.cpp
    kqp_user_request_context.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/kqp/expr_nodes
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/kqp/common/compilation
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/common/shutdown
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/tx/sharding
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/aclib
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/core/grpc_services/cancelation
    library/cpp/lwtrace
    #library/cpp/lwtrace/protos
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_tx_info.h)
GENERATE_ENUM_SERIALIZATION(kqp_yql.h)

END()

RECURSE(
    compilation
    simple
    events
)
