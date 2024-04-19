LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    all_types.proto
    all_types_proto3.proto
    clashing_enums.proto
    row.proto
)

PEERDIR(
    yt/yt_proto/yt/formats
)

END()
