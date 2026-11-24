GTEST()

SRCS(
    test.proto

    packer_ut.cpp
    unpacker_ut.cpp
)

PEERDIR(
    library/cpp/framing
)

END()
