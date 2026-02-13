PROTO_LIBRARY()

PEERDIR(
    yt/go/proto/client/table_chunk_format
    yt/go/proto/core/misc
)

ONLY_TAGS(GO_PROTO)

SRCS(
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.proto
    ${ARCADIA_ROOT}/yt/yt_proto/yt/client/table_client/proto/versioned_io_options.proto
)

END()
