PROTO_LIBRARY()

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/library/yql/providers/generic/connector/api/common
)

SRCS(
    range.proto
    source.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()

