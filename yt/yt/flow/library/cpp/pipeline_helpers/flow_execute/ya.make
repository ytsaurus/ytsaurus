LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    flow_execute.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/flow/library/cpp/client
    yt/yt/flow/library/cpp/common
    yt/yt/library/auth
    yt/yt_proto/yt/flow/controller/proto
)

END()

RECURSE_FOR_TESTS(
    unittests
)
