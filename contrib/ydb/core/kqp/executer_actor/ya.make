LIBRARY()

SRCS(
    kqp_data_executer.cpp
    kqp_scan_executer.cpp
    kqp_scheme_executer.cpp
    kqp_executer_impl.cpp
    kqp_executer_stats.cpp
    kqp_literal_executer.cpp
    kqp_locks_helper.cpp
    kqp_partition_helper.cpp
    kqp_planner.cpp
    kqp_planner_strategy.cpp
    kqp_table_resolver.cpp
    kqp_tasks_graph.cpp
    kqp_tasks_validate.cpp
    kqp_partitioned_executer.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/formats
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/compute_actor
    contrib/ydb/core/kqp/executer_actor/shards_resolver
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/gateway/local_rpc
    contrib/ydb/core/kqp/query_compiler
    contrib/ydb/core/kqp/rm_service
    contrib/ydb/core/kqp/topics
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/common
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/actors/core
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/services/metadata/abstract
)

GENERATE_ENUM_SERIALIZATION(
    kqp_executer.h
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
