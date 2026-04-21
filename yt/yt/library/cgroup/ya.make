LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    process.cpp
    statistics.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
