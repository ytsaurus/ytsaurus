LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    computed_fields_filter.cpp
    continuation.cpp
    expression_evaluator.cpp
    filter_introspection.cpp
    filter_matcher.cpp
    helpers.cpp
    misc.cpp
    query_evaluator.cpp
    query_optimizer.cpp
    query_rewriter.cpp
    type_inference.cpp
)

PEERDIR(
    yt/yt/orm/client/misc

    yt/yt/orm/library/attributes

    yt/yt/library/query/engine

    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
