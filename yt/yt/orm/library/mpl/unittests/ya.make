GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SIZE(SMALL)

SRCS(
    map_ut.cpp
    types_ut.cpp
)

PEERDIR(
    yt/yt/orm/library/mpl

    library/cpp/testing/gtest
)

END()
