LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/random_provider
    library/cpp/string_utils/parse_size
    library/cpp/iterator

    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/credentials
    yql/essentials/core/expr_nodes
    yql/essentials/core/peephole_opt
    yql/essentials/minikql
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/transform

    yt/yql/providers/ytflow/expr_nodes
    yt/yql/providers/ytflow/integration/interface
    yt/yql/providers/ytflow/integration/mkql_interface
    yt/yql/providers/ytflow/integration/proto
)

SRCS(
    yql_ytflow_configuration.cpp
    yql_ytflow_constants.cpp
    yql_ytflow_datasink.cpp
    yql_ytflow_datasink_exec.cpp
    yql_ytflow_datasink_type_ann.cpp
    yql_ytflow_datasource.cpp
    yql_ytflow_datasource_constraints.cpp
    yql_ytflow_datasource_exec.cpp
    yql_ytflow_datasource_type_ann.cpp
    yql_ytflow_logical_optimize.cpp
    yql_ytflow_physical_optimize.cpp
    yql_ytflow_physical_finalizing.cpp
    yql_ytflow_provider.cpp
    yql_ytflow_recapture.cpp
    yql_ytflow_join_utils.cpp
    yql_ytflow_swift_map.cpp
    yql_ytflow_utils.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
