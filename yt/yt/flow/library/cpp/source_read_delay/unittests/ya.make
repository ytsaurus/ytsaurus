GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    estimator_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/source_read_delay
)

SIZE(SMALL)

END()
