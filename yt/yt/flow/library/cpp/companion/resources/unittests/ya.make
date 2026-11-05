GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_resource_ut.cpp
    registry_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion/resources
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
