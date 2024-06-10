LIBRARY()

SRCS(
    column_name.cpp
    parse_config.cpp
    proto2schema.cpp
    proto2yt.cpp
    yt2proto.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson/node
    mapreduce/yt/interface
    mapreduce/yt/interface/protos
    yt/yt_proto/yt/formats
)

END()

RECURSE_FOR_TESTS(ut)
