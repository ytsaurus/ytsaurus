LIBRARY()

SRCS(
    yql_pq_datasink.cpp
    yql_pq_datasink_execution.cpp
    yql_pq_datasink_io_discovery.cpp
    yql_pq_datasink_type_ann.cpp
    yql_pq_datasource.cpp
    yql_pq_datasource_type_ann.cpp
    yql_pq_dq_integration.cpp
    yql_pq_io_discovery.cpp
    yql_pq_load_meta.cpp
    yql_pq_logical_opt.cpp
    yql_pq_mkql_compiler.cpp
    yql_pq_physical_optimize.cpp
    yql_pq_provider.cpp
    yql_pq_provider_impl.cpp
    yql_pq_settings.cpp
    yql_pq_topic_key_parser.cpp
    yql_pq_helpers.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/type_ann
    contrib/ydb/library/yql/dq/expr_nodes
    yql/essentials/core/dq_integration
    contrib/ydb/library/yql/dq/opt
    yql/essentials/minikql/comp_nodes
    yql/essentials/providers/common/config
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    yql/essentials/providers/common/dq
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    contrib/ydb/library/yql/providers/common/pushdown
    yql/essentials/providers/common/structured_token
    yql/essentials/providers/common/transform
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/generic/provider
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/pq/common
    contrib/ydb/library/yql/providers/pq/expr_nodes
    contrib/ydb/library/yql/providers/pq/proto
    yql/essentials/providers/result/expr_nodes
    yql/essentials/public/udf
    contrib/ydb/public/sdk/cpp/src/client/driver
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
