PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    credentials.proto
    file_queue.proto
    range.proto
    retry_config.proto
    sink.proto
    source.proto
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/public/api/protos
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()


