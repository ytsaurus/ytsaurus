LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    packer.cpp
    unpacker.cpp
)

PEERDIR(
    library/cpp/framing
    yt/yt/flow/library/cpp/common
)

END()

RECURSE_FOR_TESTS(unittests)
