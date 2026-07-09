GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    ordered_batching_async_sink_ut.cpp
    ordered_source_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/common/unittests/mock
)

SIZE(SMALL)

END()
