GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    pack_unpack_ut.cpp
    proto_filler.cpp
    wire_canon_ut.cpp

    proto/message_example_ut.proto
)

PEERDIR(
    yt/yt/flow/extensions/framing/cpp
    yt/yt/flow/library/cpp/common
    yt/yt/core
    yt/yt/client
    library/cpp/framing
    library/cpp/json
    library/cpp/resource
)

RESOURCE(
    ${ARCADIA_ROOT}/yt/yt/flow/extensions/framing/testdata/wire-canon.json wire-canon.json
)

END()
