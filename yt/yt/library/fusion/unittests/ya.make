GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    service_directory_ut.cpp
)

PEERDIR(
    yt/yt/library/fusion
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
