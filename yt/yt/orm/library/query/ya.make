LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    expression_evaluator.cpp
    filter_introspection.cpp
    filter_matcher.cpp
    query_evaluator.cpp
    query_optimizer.cpp
    query_rewriter.cpp
)

PEERDIR(
    yt/yt/orm/library/attributes

    yt/yt/library/query/engine

    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
