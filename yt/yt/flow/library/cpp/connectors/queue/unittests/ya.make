GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    tablet_index_evaluator_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/library/query/engine
)

SIZE(MEDIUM)

END()
