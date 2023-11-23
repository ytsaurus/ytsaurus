GTEST(unittester-yt-orm-library)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    expression_evaluator_ut.cpp
    filter_introspection_ut.cpp
    filter_matcher_ut.cpp
    query_optimizer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/library/query

    yt/yt/core/test_framework
)

END()
