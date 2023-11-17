LIBRARY()

SRCS(
    yql_generic_cluster_config.cpp
    yql_generic_datasink.cpp
    yql_generic_datasink_execution.cpp
    yql_generic_datasink_type_ann.cpp
    yql_generic_datasource.cpp
    yql_generic_datasource_type_ann.cpp
    yql_generic_dq_integration.cpp
    yql_generic_io_discovery.cpp
    yql_generic_load_meta.cpp
    yql_generic_logical_opt.cpp
    yql_generic_mkql_compiler.cpp
    yql_generic_physical_opt.cpp
    yql_generic_predicate_pushdown.cpp
    yql_generic_provider.cpp
    yql_generic_provider.h
    yql_generic_provider_impl.h
    yql_generic_settings.h
    yql_generic_settings.cpp
    yql_generic_state.h
    yql_generic_state.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/libs/fmt
    library/cpp/json
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/core/fq/libs/result_formatter
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/dq/expr_nodes
    contrib/ydb/library/yql/dq/integration
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/common/dq
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/pushdown
    contrib/ydb/library/yql/providers/common/structured_token
    contrib/ydb/library/yql/providers/common/transform
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/generic/expr_nodes
    contrib/ydb/library/yql/providers/generic/proto
    contrib/ydb/library/yql/providers/generic/connector/libcpp
)

END()

RECURSE_FOR_TESTS(ut)
