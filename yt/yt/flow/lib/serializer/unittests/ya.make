GTEST(unittester-flow-serializer)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    serializer_ut.cpp

    proto/test.proto
)

PEERDIR(
    yt/yt/client
    yt/yt/core/test_framework
    yt/yt/flow/lib/serializer
)

END()
