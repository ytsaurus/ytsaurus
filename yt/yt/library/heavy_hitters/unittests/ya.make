GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    misra_gries_ut.cpp
)

PEERDIR(
    yt/yt/library/heavy_hitters
)

SIZE(SMALL)

END()
