LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    builtin_registry.cpp
    program.cpp
    registry.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/base
)

PROVIDES(YT_QUERY_ENGINE)

END()

RECURSE_FOR_TESTS(
    unittests
)
