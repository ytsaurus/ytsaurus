GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    queue_info_ut.cpp
    registry_ut.cpp
    tablet_index_evaluator_ut.cpp
)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/library/query/engine
)

SIZE(MEDIUM)

END()
