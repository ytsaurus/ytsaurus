G_BENCHMARK()

SRCS(
    test.proto

    packer_bench.cpp
    unpacker_bench.cpp
)

PEERDIR(
    library/cpp/framing
)

END()
