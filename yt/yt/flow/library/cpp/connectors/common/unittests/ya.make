GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    async_at_most_once_sink_ut.cpp
    ordered_batching_async_sink_ut.cpp
    ordered_source_ut.cpp
    sync_replica_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/common/unittests/mock
)

SIZE(SMALL)

END()
