GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    endpoint_provider_ut.cpp
    node_info_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/runner
    yt/yt/library/program
)

SIZE(SMALL)

END()
