LIBRARY()

SRCS(
    create_proto.cpp
    data_ptr.cpp
    hash.cpp
    state.cpp
    merge.cpp
)

PEERDIR(
    library/cpp/xdelta3/proto
    library/cpp/xdelta3/xdelta_codec
)

END()
