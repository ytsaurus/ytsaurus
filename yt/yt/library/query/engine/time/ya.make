LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dates.cpp
)

PEERDIR(
    yt/yt/library/query/engine_api
)

END()

RECURSE_FOR_TESTS(
    unittests
)
