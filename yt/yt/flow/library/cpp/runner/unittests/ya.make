GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    endpoint_provider_ut.cpp
    node_info_ut.cpp
    root_clients_cache_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/misc/testing
    yt/yt/flow/library/cpp/runner
    yt/yt/library/program
)

SIZE(SMALL)

END()
