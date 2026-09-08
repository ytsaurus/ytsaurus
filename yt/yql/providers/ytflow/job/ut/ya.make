GTEST()

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/common

    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/public/udf/service/exception_policy

    yt/yql/providers/ytflow/common
    yt/yql/providers/ytflow/job
    yt/yt/core
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/resources
    yt/yt/library/profiling/solomon
)

DEPENDS(
    yql/essentials/udfs/test/simple
    yt/yql/providers/ytflow/job/ut/concurrent_pattern_build_udf
)

SRCS(
    yql_ytflow_computation_pattern_ut.cpp
    yql_ytflow_function_registry_ut.cpp
    yql_ytflow_map_validation_ut.cpp
    yql_ytflow_resources_ut.cpp
)

END()

RECURSE(
    concurrent_pattern_build_udf
)
