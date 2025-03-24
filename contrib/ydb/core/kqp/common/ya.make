LIBRARY()

SRCS(
    control.cpp
    kqp_event_ids.h
    kqp_event_impl.cpp
    kqp_lwtrace_probes.cpp
    kqp_lwtrace_probes.h
    kqp_resolve.cpp
    kqp_resolve.h
    kqp_ru_calc.cpp
    kqp_script_executions.cpp
    kqp_timeouts.cpp
    kqp_timeouts.h
    kqp_tx_manager.cpp
    kqp_tx.cpp
    kqp_types.cpp
    kqp_types.h
    kqp_user_request_context.cpp
    kqp_user_request_context.h
    kqp_yql.cpp
    kqp_yql.h
    kqp.cpp
    kqp.h
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
    yql/essentials/core/issue
    yql/essentials/core/services
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/common
    yql/essentials/core/dq_integration
    yql/essentials/parser/pg_wrapper/interface
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/sdk/cpp/src/library/operation_id/protos
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
    events
    simple
)
