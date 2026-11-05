GTEST()

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/services
    yql/essentials/core/type_ann
    yql/essentials/providers/common/config/transformer
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yt/yql/providers/ytflow/expr_nodes
    yt/yql/providers/ytflow/integration/interface
    yt/yql/providers/ytflow/provider
)

SRCS(
    yql_ytflow_datasink_exec_ut.cpp
    yql_ytflow_logical_optimize_ut.cpp
    yql_ytflow_physical_finalizing_setup.cpp
    yql_ytflow_physical_finalizing_ut.cpp
    yql_ytflow_physical_optimize_ut.cpp
    yql_ytflow_utils_ut.cpp
)

END()
