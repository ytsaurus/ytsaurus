GTEST(unittester-library-query-portable-engine)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    builtin_registry_ut.cpp
    program_ut.cpp
    query_evaluator_ut.cpp
    registry_ut.cpp
    semantics_cases.cpp
    semantics_ut.cpp
)

RESOURCE(
    ../capabilities.yson portable_expression_capabilities
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/query/portable_engine
)

SIZE(SMALL)

END()

RECURSE(
    allocation
)
