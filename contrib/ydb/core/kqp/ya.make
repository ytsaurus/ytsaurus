LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    contrib/ydb/library/actors/wilson
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/engine
    contrib/ydb/core/formats
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/compile_service
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/kqp/executer_actor
    contrib/ydb/core/kqp/expr_nodes
    contrib/ydb/core/kqp/gateway
    contrib/ydb/core/kqp/host
    contrib/ydb/core/kqp/node_service
    contrib/ydb/core/kqp/opt
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/kqp/proxy_service
    contrib/ydb/core/kqp/query_compiler
    contrib/ydb/core/kqp/rm_service
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/kqp/session_actor
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/service
    contrib/ydb/core/util
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/aclib
    yql/essentials/core/services/mounts
    yql/essentials/public/issue
    contrib/ydb/library/yql/utils/actor_log
    yql/essentials/utils/log
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/base
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    contrib/ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt
)

END()

RECURSE(
    common
    compile_service
    compute_actor
    counters
    executer_actor
    expr_nodes
    federated_query
    finalize_script_service
    gateway
    host
    node_service
    opt
    provider
    proxy_service
    rm_service
    run_script_actor
    runtime
    session_actor
    tests
    workload_service
)

RECURSE_FOR_TESTS(
    ut
    tools/combiner_perf/bin
)
