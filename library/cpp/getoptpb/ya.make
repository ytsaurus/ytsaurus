LIBRARY()

SRCS(
    camel2hyphen.cpp
    getoptpb.cpp
    util.cpp
    camel2hyphen.h
    getoptpb.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    library/cpp/getoptpb/proto
    library/cpp/getopt/small
    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(ut)
