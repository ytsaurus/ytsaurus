LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    builtin_registry.cpp
    evaluation_helpers.cpp
    program.cpp
    GLOBAL query_evaluator.cpp
    registry.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/base
    yt/yt/library/query/engine_api
)

PROVIDES(YT_QUERY_ENGINE)

END()

RECURSE_FOR_TESTS(
    unittests
)
