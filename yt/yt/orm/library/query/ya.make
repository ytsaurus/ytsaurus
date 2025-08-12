LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    computed_fields_filter.cpp
    continuation.cpp
    enforce_aggregate.cpp
    filter_introspection.cpp
    helpers.cpp
    misc.cpp
    query_optimizer.cpp
    query_rewriter.cpp
    split_filter.cpp
    type_inference.cpp
)

PEERDIR(
    yt/yt/orm/client/misc

    yt/yt/orm/library/attributes

    yt/yt/library/query/base

    yt/yt/core
    yt/yt/client
)

END()

RECURSE(
    heavy
)

RECURSE_FOR_TESTS(
    unittests
)
