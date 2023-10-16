LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    clickhouse_service.proto
    subquery_spec.proto
)

PEERDIR(
    yt/yt/ytlib
    yt/yt_proto/yt/core
)

END()
