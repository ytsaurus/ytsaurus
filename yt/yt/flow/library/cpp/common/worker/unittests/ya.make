GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    message_id_batch_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common/worker
)

SIZE(MEDIUM)

END()
