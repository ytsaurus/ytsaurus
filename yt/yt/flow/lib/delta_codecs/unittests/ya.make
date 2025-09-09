GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    xdelta_ut.cpp
)

PEERDIR(
    yt/yt/flow/lib/delta_codecs
)

SIZE(SMALL)

END()
