LIBRARY()

SRCS(
    proto_yson_struct.cpp
    serializer.cpp
)

PEERDIR(
    library/cpp/yt/yson
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(unittests)
