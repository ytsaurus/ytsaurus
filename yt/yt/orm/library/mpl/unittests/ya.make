GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SIZE(SMALL)

SRCS(
    map_ut.cpp
    types_ut.cpp
)

PEERDIR(
    yt/yt/orm/library/mpl

    yt/yt/core/test_framework
)

END()
