GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    binary_checksum_override_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
)

ENV(YT_FLOW_BINARY_CHECKSUM_OVERRIDE="overridden-for-test")

SIZE(SMALL)

END()
