LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    estimator.cpp
)

PEERDIR(
    util
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
