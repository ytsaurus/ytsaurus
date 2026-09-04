GTEST()

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yt/yql/providers/ytflow/expr_nodes
    yt/yql/providers/ytflow/provider
)

SRCS(
    yql_ytflow_datasink_exec_ut.cpp
    yql_ytflow_physical_finalizing_setup.cpp
    yql_ytflow_physical_finalizing_ut.cpp
)

END()
