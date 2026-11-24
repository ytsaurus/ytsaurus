LIBRARY()

PEERDIR(
    contrib/libs/protobuf
)

SRCS(
    packer.cpp
    syncword.cpp
    unpacker.cpp
    utils.cpp
)

GENERATE_ENUM_SERIALIZATION(format.h)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
