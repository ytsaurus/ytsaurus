GTEST()

SRCS(
    schema_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    library/cpp/testing/gtest
    yt/yt/client/arrow
)

END()
