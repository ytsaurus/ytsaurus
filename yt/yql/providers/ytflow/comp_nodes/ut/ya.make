GTEST()

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    library/cpp/time_provider
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yt/yql/providers/yt/mkql_ytflow
    yt/yql/providers/ytflow/comp_nodes
    yt/yql/providers/ytflow/integration/mkql_interface
)

SRCS(
    mkql_ytflow_lookup_join_ut.cpp
)

END()
