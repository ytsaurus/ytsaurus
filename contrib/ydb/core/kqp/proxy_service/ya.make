LIBRARY()

SRCS(
    kqp_proxy_service.cpp
    kqp_proxy_databases_cache.cpp
    kqp_proxy_peer_stats_calculator.cpp
    kqp_script_execution_retries.cpp
    kqp_script_executions.cpp
    kqp_session_info.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    contrib/ydb/core/kqp/proxy_service/proto
    contrib/ydb/core/kqp/run_script_actor
    contrib/ydb/core/kqp/workload_service
    contrib/ydb/core/mind
    contrib/ydb/core/protos
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/mon
    contrib/ydb/library/query_actor
    contrib/ydb/library/table_creator
    contrib/ydb/library/yql/providers/common/http_gateway
    yql/essentials/providers/common/proto
    contrib/ydb/library/yql/providers/s3/actors_factory
    yql/essentials/public/issue
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/operation_id
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/sdk/cpp/src/client/params
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
