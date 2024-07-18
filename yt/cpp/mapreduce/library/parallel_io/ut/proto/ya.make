LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    data.proto
    test_message.proto
)

PEERDIR(
    yt/yt_proto/yt/formats
)

END()
