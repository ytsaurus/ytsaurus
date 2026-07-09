GTEST(unittester-flow-delta-codecs)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    xdelta_ut.cpp
)

PEERDIR(
    yt/yt/flow/lib/delta_codecs
)

SIZE(SMALL)

END()
