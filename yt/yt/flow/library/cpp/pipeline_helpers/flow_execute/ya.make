LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    flow_execute.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/flow/library/cpp/common
)

END()

RECURSE_FOR_TESTS(
    unittests
)
